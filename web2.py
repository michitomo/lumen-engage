import os
import json
import base64
import asyncio
import time # Add for timestamp-based cleanup
from fastapi import FastAPI, WebSocket, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
from fastapi.websockets import WebSocketDisconnect
from pydantic import BaseModel
from twilio.rest import Client
import websockets
import websockets.protocol
from dotenv import load_dotenv
import uvicorn
import re
import urllib.parse
from datetime import datetime
import pytz # Need to install: pip install pytz
import html # Add this import for XML escaping
import uuid

# Global dictionary to store call data temporarily
CALL_DATA_STORAGE = {}
CALL_DATA_TIMEOUT_SECONDS = 300 # 5 minutes timeout for cleanup

load_dotenv(override=True)

# Configuration
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')
PHONE_NUMBER_FROM = os.getenv('PHONE_NUMBER_FROM')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
raw_domain = os.getenv('DOMAIN', '')
# Strip protocols and trailing slashes from DOMAIN for WebSocket URL
DOMAIN = re.sub(r'(^\w+:|^)\/\/|\/+$', '', raw_domain) 

PORT = int(os.getenv('PORT', 6060))

# Updated SYSTEM_MESSAGE based on the product concept and AI persona "ルーメン"
# This is the core instruction for the AI's behavior
SYSTEM_MESSAGE = (
    "あなたはLumen EngageシステムのAIアシスタント、「ルーメン」です。"
    "あなたのミッションは、弊社LPから情報をご登録いただいたお客様に対し、迅速な電話コンタクトを通じて、"
    "**お客様の現状のウェブサイトからの問い合わせ対応やフォーム運用に関する状況・課題**をヒアリングし、"
    "Lumen Engageがどのように貢献できるか、最適な次のステップ（主に営業担当者との面談）へと繋げることです。"
    "\n\n"
    "**あなたのペルソナ・口調:**\n"
    "*   プロフェッショナルかつスムーズな対応を心がけてください。\n"
    "*   声のトーンは明るく、聞き取りやすいスピードで話してください。\n"
    "*   お客様に寄り添い、お客様に話していただくことを重視する傾聴力と、簡潔かつ的確な質問で情報を引き出す能力、そしてヒアリング内容に基づき最適な次のステップを積極的に提案する推進力を持ち合わせてください。\n"
    "*   丁寧さと効率性を両立させ、洗練された会話を目指してください。\n"
    "*   **あなたの発話はできるだけ短く、お客様の発言を促すことに集中してください。**\n"
    "\n"
    "**会話の目的:**\n"
    "1.  お客様がLPにご登録いただいたことへの感謝を伝える。\n"
    "2.  お客様のお名前（**苗字のみ**）と会社名を確認する。\n"
    "3.  **お客様の現在のウェブサイト/LPからの問い合わせ対応やフォーム運用の状況（担当者、使用ツール、課題など）**についてヒアリングする。\n"
    "4.  ヒアリング内容に基づき、お客様にとって最も有益となる次のステップ（例: 営業担当者との面談、個別デモ、詳細資料のご提供など）を積極的に提案し、合意を得る。\n"
    "5.  面談となった場合、本日の日付と平日9:00から22:00（日本標準時 JST）の空き時間帯を考慮して、お客様のご都合の良い候補日時をいくつか提案し、日程を確定させる。（カレンダー連携はないため、適当な候補日を提案し、確定後の調整は別途行うことを示唆しても良い）\n"
    "6.  会話で得られた重要な情報（ヒアリング内容、確定した次ステップ、確定日程）を記録する。\n"
    "\n"
    "**会話の基本フロー:**\n"
    "1.  **オープニング:** 「Lumen EngageのAIアシスタント、ルーメン」であること、LP登録に関する連絡であること、お客様のお名前と会社名を確認することを簡潔に伝える。「[会社名]の[苗字]様でいらっしゃいますでしょうか？」と問いかける。\n"
    "2.  **時間確認:** 名前・会社名が確認できたら、**簡潔に**「今、少しお時間よろしいでしょうか？」と時間があるか確認する。\n"
    "3.  **ヒアリング（最重要フェーズ）:** 時間確認が取れたら、「先ほどは弊社ウェブサイトよりお問い合わせ（または資料ダウンロード等）いただきありがとうございます。差し支えなければ、**現在、貴社ではウェブサイトからのお問い合わせやフォームに、どのようにご対応されているか**、簡単にお伺いしてもよろしいでしょうか？」と最初の質問を投げかけ、フォーム対応状況に関するヒアリングを開始する。LP登録のトリガー（お問い合わせ or 資料ダウンロードなど）は会話に含めても良い。\n"
    "4.  **詳細ヒアリング（営業担当として深掘り）:** お客様の発言に対し、**一度に一つの質問のみ**で、さらに深掘りして以下の点を引き出すよう、積極的に質問を組み立てる。\n"
    "    *   ウェブサイトからのお問い合わせ対応は、**主にどなたが担当されているか**？ (例: 営業担当、マーケティング担当、事務担当など)\n"
    "    *   お問い合わせ内容の管理や、お客様への返信には、**どのようなツールやシステムを使っているか**？ (例: メールソフト、スプレッドシート、CMS付属機能、SFA/CRMなど)\n"
    "    *   現在、問い合わせ対応やフォーム運用に関して、**何か課題や手間になっていること**はあるか？\n"
    "    *   （可能であれば）お問い合わせからお客様への最初の返信まで、**平均してどのくらいの時間がかかっているか**？\n"
    "    *   （可能であれば）現在のお問い合わせ対応の体制で、**人員的な余裕はどのくらいあるか**？\n"
    "5.  **最適な次ステップの提案（積極的な意思決定）:** ヒアリングで得られた情報（特に課題や担当者の状況、システム利用状況）を総合的に判断し、お客様にとって最も価値のある次のアクションを「ルーメン」自身が積極的に提案する。提案する際は、Lumen Engageがお客様の課題（例: 対応時間短縮、担当者の負荷軽減、情報の一元管理など）をどのように解決できるか簡潔に言及する。\n"
    "    *   例えば、担当者の負荷が高い、システムがアナログ（メール、スプレッドシートなど）であれば、「ルーメン」による自動化が有効であることを示唆し、詳細を説明する面談を提案する。\n"
    "    *   CMS連携の課題などがあれば、Lumen Engageの連携機能をデモできる面談を提案する。\n"
    "    *   まだ漠然とした情報収集であれば、「まずはLumen Engageの具体的な機能や導入事例をまとめた資料をメールで送らせていただきます」と代替案を提示する。\n"
    "    *   提案に対し、お客様の意向を確認し、次のステップを確定させる。\n"
    "6.  **日程調整（面談の場合）:** 面談となった場合、本日の日付（会話開始時に伝える）と平日9:00から22:00（JST）の条件を考慮し、お客様のご都合の良い候補日時をいくつか**一度に一つずつ提示するのではなく、複数の候補を分かりやすく提示し**、調整を進める。オンラインかオフライン（もしLPで選択肢があれば）かも確認し、確定する。\n"
    "7.  **確認・クロージング:** 確定した次ステップ（面談日時、形式など）を復唱し、お客様に確認していただく。会話で伺った内容（特にフォーム対応状況に関する情報）も**簡潔に**復唱し、認識のずれがないか確認しても良い。ご協力いただいたことへの感謝を伝え、通話を終了する。\n"
    "\n"
    "**禁止事項・制約:**\n"
    "*   **お客様の会話を遮らないでください。** 必ずお客様の発言が終わるのを待ってから話してください。\n"
    "*   **一度に複数の質問をしないでください。お客様の発言を受けてから、次の質問（または応答）を行ってください。**\n"
    "*   **あなたの発話はできるだけ短く、簡潔にしてください。** お客様に多く話していただくことを目指してください。\n"
    "*   不必要な雑談や、役割を超える詳細な技術的な説明は避けてください。あなたの主目的はヒアリングと次ステップへの誘導、日程調整です。\n"
    "*   お客様が会話を中断したい、または今は忙しいと仰った場合は、速やかに感謝を伝え、引き下がってください。無理に引き止めないでください。その際は、「改めてメールで情報をお送りしましょうか？」など代替案を提案しても良い。\n"
    "*   聞き取れなかった場合は、丁寧に聞き返してください。(例: 「恐れ入ります、少々聞き取りづらかったのですが、もう一度お願いできますでしょうか？」)\n"
    "*   想定外の質問や、あなたの役割を超える質問には、正直に「私はAIアシスタントのルーメンですので、詳細については担当から改めてご連絡差し上げます」のように丁寧にお断りし、話題を会話の目的に戻してください。ただし、営業担当として回答できる範囲の一般的な質問には積極的に回答してください。\n"
    "*   会社名を会話の冒頭以外で繰り返し名乗る必要はありません。あなたは「ルーメン」として会話を進めてください。\n"
    "*   お客様の名前を呼ぶ際は、**必ず苗字のみ**を使用してください。（例: 山田様）\n"
    "\n"
    "**重要:** 会話のすべての情報（特に、**フォーム対応担当者、使用ツール、課題、対応時間、人員状況**といったヒアリング内容、お客様の発言、確定した次ステップ、確定日程）は、後続の営業担当者やシステムに引き継がれるよう、会話内で得られた情報を整理してください（AIは記録システムと連携する前提で）。"
)

# Removed specific voice instruction from SYSTEM_MESSAGE as VOICE variable is used
VOICE = 'alloy' # Use a fixed voice as specified

LOG_EVENT_TYPES = [
    'error', 'response.content.done', 'rate_limits.updated', 'response.done',
    'input_audio_buffer.committed', 'input_audio_buffer.speech_stopped',
    'input_audio_buffer.speech_started', 'session.created'
]

app = FastAPI()

if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and PHONE_NUMBER_FROM and OPENAI_API_KEY):
    raise ValueError('Missing Twilio and/or OpenAI environment variables. Please set them in the .env file.')

# Initialize Twilio client
client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

# Pydantic models for request/response
class CallRequest(BaseModel):
    phone_number: str
    # custom_message: str = None # Removed as AI persona/opening is defined in prompt
    voice: str = "alloy" # Keep as it might be used in init_session, though fixed to alloy now
    name: str # Make name and company_name required based on form fields
    company_name: str

class CallResponse(BaseModel):
    success: bool
    call_sid: str = None
    message: str

@app.get('/', response_class=JSONResponse)
async def index_page():
    return {"message": "Twilio Media Stream Server is running!", "endpoints": {
        "make_call": "POST /make-call",
        "media_stream": "WebSocket /media-stream"
    }}

@app.post('/make-call', response_model=CallResponse)
async def make_call_webhook(call_request: CallRequest, background_tasks: BackgroundTasks):
    """Webhook endpoint to initiate an outbound call."""
    try:
        if not call_request.phone_number:
            raise HTTPException(status_code=400, detail="Phone number is required")
        if not call_request.name or not call_request.company_name:
             raise HTTPException(status_code=400, detail="Name and Company Name are required")


        # Validate phone number format (basic validation)
        # Adding plus if missing, required for Twilio often
        phone_number_to_call = call_request.phone_number.replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
        if not phone_number_to_call.startswith('+'):
            # Basic assumption for US numbers, adjust as needed for other regions
            # For production, more robust E.164 validation is recommended
             if len(phone_number_to_call) == 10: # Simple check for 10 digits (like US without country code)
                 phone_number_to_call = "+1" + phone_number_to_call # Assuming +1 prefix for US
             else:
                 raise HTTPException(status_code=400, detail="Invalid phone number format. Ensure it includes country code with +.")

        if not re.match(r'^\+?[1-9]\d{7,14}$', phone_number_to_call): # E.164 basic pattern
            raise HTTPException(status_code=400, detail="Invalid phone number format. Please use E.164 format.")

        # Check if number is allowed
        #is_allowed = await check_number_allowed(phone_number_to_call)
        # For development, override this check
        is_allowed = True # Assuming check is successful for prototype
        
        if not is_allowed:
            raise HTTPException(
                status_code=403, 
                detail=f"The number {phone_number_to_call} is not recognized as a valid outgoing number or caller ID."
            )

        # Get current date in JST for AI prompt
        jst = pytz.timezone('Asia/Tokyo')
        current_date_jst = datetime.now(jst).strftime('%Y年%m月%d日') # Format as 'YYYY年MM月DD日'

        # Create the call
        call_sid = await initiate_call(
            phone_number_to_call,
            call_request.voice, # voice argument is still used by initiate_call
            call_request.name,
            call_request.company_name,
            current_date_jst # Pass current date
        )
        
        # Log the call in background
        background_tasks.add_task(log_call_sid, call_sid)
        
        return CallResponse(
            success=True,
            call_sid=call_sid,
            message=f"Call initiated successfully to {phone_number_to_call}"
        )

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error making call: {e}")
        # Provide a more user-friendly error message while logging detailed error
        raise HTTPException(status_code=500, detail="Failed to initiate call. Please check the phone number and try again.")

@app.websocket('/media-stream')
async def handle_media_stream(websocket: WebSocket):
    """Handle WebSocket connections between Twilio and OpenAI."""
    print("Client connected")
    await websocket.accept()

    # Extract query parameters from the WebSocket scope
    query_params = dict(websocket.query_params)
    print(f"Received WebSocket query parameters: {query_params}")
    call_id = query_params.get('call_id', None)
    
    # Retrieve call data from storage
    call_data = None
    if call_id and call_id in CALL_DATA_STORAGE:
        call_data = CALL_DATA_STORAGE[call_id]
        print(f"Retrieved call data for call_id {call_id}: {call_data}")
    else:
        # Fallback to the most recent entry if call_id is not available or not found
        if CALL_DATA_STORAGE:
            # Get the most recent entry based on timestamp
            call_data = max(CALL_DATA_STORAGE.values(), key=lambda x: x['timestamp'], default=None)
            print(f"Fallback to most recent call data: {call_data}")
        else:
            print("No call data found in storage, using default values.")
    
    # Use retrieved data or defaults
    customer_name = call_data['name'] if call_data else 'お客様'
    company_name = call_data['company_name'] if call_data else '貴社'
    current_date_jst = call_data['current_date_jst'] if call_data else '本日'

    async with websockets.connect(
        'wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-12-17', # Use the recommended model for Realtime
        additional_headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "OpenAI-Beta": "realtime=v1"
        }
    ) as openai_ws:
        # Use the fixed VOICE here
        await initialize_session(openai_ws, voice=VOICE, customer_name=customer_name, company_name=company_name, current_date_jst=current_date_jst) 
        stream_sid = None

        # Send initial conversation item with customer info and date - moved to initialize_session
        # await send_initial_conversation_item(openai_ws, customer_name=customer_name, company_name=company_name, current_date_jst=current_date_jst)

        async def receive_from_twilio():
            """Receive audio data from Twilio and send it to the OpenAI Realtime API."""
            nonlocal stream_sid
            try:
                async for message in websocket.iter_text():
                    data = json.loads(message)
                    if data['event'] == 'media' and openai_ws.state == websockets.protocol.State.OPEN:
                        audio_append = {
                            "type": "input_audio_buffer.append",
                            "audio": data['media']['payload']
                        }
                        await openai_ws.send(json.dumps(audio_append))
                    elif data['event'] == 'start':
                        stream_sid = data['start']['streamSid']
                        print(f"Incoming stream has started {stream_sid}")
                    elif data['event'] == 'stop': # Handle call end
                         print(f"Twilio stream {stream_sid} stopped.")
                         if openai_ws.state == websockets.protocol.State.OPEN:
                             await openai_ws.close() # Close OpenAI WebSocket
                             print("Closed OpenAI WebSocket.")

            except WebSocketDisconnect:
                print("Client disconnected (WebSocketDisconnect).")
                if openai_ws.open:
                    await openai_ws.close() # Close OpenAI WebSocket
                    print("Closed OpenAI WebSocket on client disconnect.")
            except Exception as e:
                print(f"Error in receive_from_twilio: {e}")
                if openai_ws.open:
                    try:
                         await openai_ws.close() # Attempt to close OpenAI WebSocket
                         print("Closed OpenAI WebSocket due to error.")
                    except Exception as close_e:
                         print(f"Error closing OpenAI WebSocket: {close_e}")


        async def send_to_twilio():
            """Receive events from the OpenAI Realtime API, send audio back to Twilio."""
            nonlocal stream_sid
            try:
                async for openai_message in openai_ws:
                    response = json.loads(openai_message)
                    if response['type'] in LOG_EVENT_TYPES:
                        # Log important events, avoid excessive logging of audio delta
                        if response['type'] != 'response.audio.delta':
                             print(f"Received event: {response['type']}", response)
                    
                    if response['type'] == 'session.updated':
                        print("Session updated successfully:", response)
                        
                    if response['type'] == 'response.audio.delta' and response.get('delta'):
                        try:
                            # Process audio payload the same way as the working web.py
                            audio_payload = base64.b64encode(base64.b64decode(response['delta'])).decode('utf-8')
                            
                            # Ensure stream_sid is available before sending media
                            if stream_sid:
                                audio_delta = {
                                    "event": "media",
                                    "streamSid": stream_sid,
                                    "media": {
                                        "payload": audio_payload
                                    }
                                }
                                await websocket.send_json(audio_delta)
                            # else: # Optional: log if stream_sid is not available
                            #    print("Warning: stream_sid not available, cannot send audio_delta.")

                        except Exception as e:
                            print(f"Error processing audio data: {e}")
            except websockets.exceptions.ConnectionClosedOK:
                 print("OpenAI WebSocket closed gracefully.")
            except Exception as e:
                print(f"Error in send_to_twilio: {e}")
                # If this loop exits due to error, try to close the Twilio WebSocket
                try:
                    await websocket.close()
                    print("Closed Twilio WebSocket due to error in send_to_twilio.")
                except Exception as close_e:
                     print(f"Error closing Twilio WebSocket: {close_e}")


        # Ensure both tasks run concurrently
        await asyncio.gather(receive_from_twilio(), send_to_twilio())


async def send_initial_conversation_item(openai_ws, customer_name: str, company_name: str, current_date_jst: str):
    """Send initial conversation so AI talks first, including customer info and date."""

    # Extract surname from customer_name (simple splitting by space)
    # Note: This is a basic approach and might not work for all naming conventions.
    # A more robust approach might be needed for production.
    surname = customer_name.split(' ')[0] if customer_name else 'お客様' # Use surname or default

    # Construct the initial instruction/context for the AI
    # We provide the customer info and today's date as context for the AI to use in the conversation
    context_message = (
        f"電話が接続されました。お客様は**{company_name}の{customer_name}様**です。ただし、会話中にお客様の名前を呼ぶ際は、**必ず苗字のみ（「{surname}様」のように）**を使用してください。"
        f"LPからのお問い合わせに対する電話であることを伝えてください。"
        f"**本日は{current_date_jst}**です。この日付情報を会話の中で適切に利用してください（例: 日程調整時）。"
        f"システムプロンプトに記載されている役割とフローに従い、会話を開始してください。"
        f"最初の目的は、お客様のお名前（苗字）と会社名を確認することです。"
        f"具体的には、**あなたの最初の発話は短く**、「Lumen Engageのアシスタント、ルーメンです。[会社名]の{surname}様でいらっしゃいますでしょうか？」と問いかけるところから始めてください。"
    )

    initial_conversation_item = {
        "type": "conversation.item.create",
        "item": {
            "type": "message",
            "role": "user", # Send the initial instruction/context as a user message to guide the AI's first response
            "content": [
                {
                    "type": "input_text",
                    "text": context_message
                }
            ]
        }
    }
    await openai_ws.send(json.dumps(initial_conversation_item))

    # Request the AI to generate a response based on the conversation items provided so far
    await openai_ws.send(json.dumps({"type": "response.create"}))


async def initialize_session(openai_ws, voice: str = "alloy", customer_name: str = "お客様", company_name: str = "貴社", current_date_jst: str = "本日"):
    """Control initial session with OpenAI."""
    session_update = {
        "type": "session.update",
        "session": {
            "turn_detection": {"type": "server_vad"}, # Use server-side VAD for turn detection
            # Audio format must match Twilio Media Stream format
            "input_audio_format": "g711_ulaw", 
            "output_audio_format": "g711_ulaw",
            "voice": voice, # Use the specified voice
            "instructions": SYSTEM_MESSAGE, # Pass the main system instructions
            "modalities": ["text", "audio"],
            "temperature": 0.8, # Adjust temperature as needed (0.0 to 1.0)
            # Add additional session parameters if needed
            # e.g., "endpoint_options": {"type": "never"}, # Keep connection open
            # "model_options": {"model": "gpt-4o-realtime-preview-2024-12-17"} # Explicitly set model
        }
    }
    print('Sending session update:', json.dumps(session_update))
    await openai_ws.send(json.dumps(session_update))

    # The AI will speak first based on the 'conversation.item.create' sent after session initialization
    # Send initial conversation item with customer info and date
    await send_initial_conversation_item(openai_ws, customer_name=customer_name, company_name=company_name, current_date_jst=current_date_jst)

# This function remains largely the same, but added logging and error handling
async def check_number_allowed(to):
    """Check if a number is allowed to be called."""
    try:
        # Placeholder for actual number validation/checking logic
        # You might check against a list of approved numbers, or Twilio capabilities
        print(f"Checking if number {to} is allowed...")
        # In a real scenario, implement checks like:
        # 1. Is this an E.164 formatted number? (Already done in make_call_webhook)
        # 2. Do you have permission to call this number? (e.g., is it in your CRM as an opt-in contact?)
        # 3. Is this a known invalid number (e.g., premium rate, internal, etc.)?

        # For the prototype, we are always returning True as overridden in make_call_webhook
        # The original code checked Twilio IncomingPhoneNumbers or OutgoingCallerIds,
        # which might not be the correct check for *calling* a customer number.
        # A customer number is usually not one of your Twilio numbers or caller IDs.
        # Returning True here bypasses that check based on the original override.
        return True 
    except Exception as e:
        print(f"Error checking phone number {to}: {e}")
        # Return False in case of error for safety
        return False

async def initiate_call(phone_number_to_call: str, voice: str, name: str, company_name: str, current_date_jst: str):
    """Make an outbound call."""
    if not phone_number_to_call:
        raise ValueError("Please provide a phone number to call.")

    # Ensure compliance with applicable laws and regulations
    # All of the rules of TCPA apply even if a call is made by AI.
    # Do your own diligence for compliance.

    # Construct WebSocket URL with query parameters
    # Pass name, company_name, and current_date_jst via query parameters
    ws_url = f"wss://{DOMAIN}/media-stream"
    # Generate a unique call ID for this call (using call SID later)
    temp_call_id = str(uuid.uuid4())
    query_params = {
        'call_id': temp_call_id
        # Removed other params since they might not be passed by Twilio
    }

    # Encode query parameters with proper handling of special characters
    encoded_query_params = '&'.join(f"{key}={urllib.parse.quote(value)}" for key, value in query_params.items())
    ws_url_with_params = f"{ws_url}?{encoded_query_params}"
    
    # Properly escape the URL for XML
    escaped_ws_url = html.escape(ws_url_with_params)
    print(f"WebSocket URL sent to Twilio: {escaped_ws_url}") # Log the exact URL

    # Store call data in global storage with timestamp for cleanup
    CALL_DATA_STORAGE[temp_call_id] = {
        'name': name,
        'company_name': company_name,
        'current_date_jst': current_date_jst,
        'timestamp': time.time()
    }
    print(f"Stored call data for call_id {temp_call_id}: {CALL_DATA_STORAGE[temp_call_id]}")

    outbound_twiml = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<Response><Connect><Stream url="{escaped_ws_url}" /></Connect></Response>'
    )
    print(f"TwiML: {outbound_twiml}")

    call = client.calls.create(
        from_=PHONE_NUMBER_FROM,
        to=phone_number_to_call,
        twiml=outbound_twiml
    )

    # Update the storage with the actual call SID if needed
    CALL_DATA_STORAGE[call.sid] = CALL_DATA_STORAGE[temp_call_id].copy()
    CALL_DATA_STORAGE[call.sid]['call_id'] = temp_call_id
    print(f"Updated call data with SID {call.sid}")

    # Clean up old entries in CALL_DATA_STORAGE
    cleanup_call_data_storage()

    return call.sid

def cleanup_call_data_storage():
    """Remove old entries from CALL_DATA_STORAGE based on timestamp."""
    current_time = time.time()
    expired_keys = [key for key, data in CALL_DATA_STORAGE.items() if current_time - data['timestamp'] > CALL_DATA_TIMEOUT_SECONDS]
    for key in expired_keys:
        del CALL_DATA_STORAGE[key]
        print(f"Cleaned up expired call data for key {key}")

async def log_call_sid(call_sid):
    """Log the call SID."""
    print(f"Call started with SID: {call_sid}")
    # In a real application, you would log this to a database, monitoring system, etc.

@app.get('/health')
async def health_check():
    """Health check endpoint."""
    # Basic check, could be expanded to check external dependencies like Twilio/OpenAI status
    return {"status": "healthy", "service": "Lumen Engage AI Voice Assistant"}

if __name__ == "__main__":
    print(
        'Lumen Engage AI Voice Assistant Webhook Server\n'
        'NOTE: This is a prototype. Ensure proper legal/compliance review before production use.\n'
        'Our recommendation is to always disclose the use of AI for outbound or inbound calls.\n'
        'Reminder: All of the rules of TCPA apply even if a call is made by AI.\n'
        'Check with your counsel for legal and compliance advice.\n'
        f'Server will start on port {PORT}\n'
        'Endpoints:\n'
        '  POST /make-call - Initiate a phone call\n'
        '  GET /health - Health check\n'
        '  WebSocket /media-stream - Media stream handler\n'
    )
    
    uvicorn.run(app, host="0.0.0.0", port=PORT)