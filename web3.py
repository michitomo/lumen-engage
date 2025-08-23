import os
import json
import base64
import asyncio
import time # Add for timestamp-based cleanup
from fastapi import FastAPI, WebSocket, BackgroundTasks, HTTPException, Header
from fastapi.responses import JSONResponse
from fastapi.websockets import WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware  # Add CORS middleware
from pydantic import BaseModel
from twilio.rest import Client
import websockets
import websockets.protocol
from dotenv import load_dotenv
import uvicorn
import re
import urllib.parse
from datetime import datetime, timezone
import pytz # Need to install: pip install pytz
import html # Add this import for XML escaping
import uuid
import aiohttp # Add for Discord webhook notifications

# Global dictionary to store call data temporarily
CALL_DATA_STORAGE = {}
CALL_DATA_TIMEOUT_SECONDS = 300 # 5 minutes timeout for cleanup

# Global dictionary to store conversation data for Discord notifications
CONVERSATION_DATA = {}

load_dotenv(override=True)

# Configuration
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')
PHONE_NUMBER_FROM = os.getenv('PHONE_NUMBER_FROM')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
raw_domain = os.getenv('DOMAIN', '')

# Discord Configuration
# Add these to your .env file:
# DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/YOUR_WEBHOOK_URL"
# DISCORD_NOTIFICATIONS_ENABLED=true
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL', '')
DISCORD_NOTIFICATIONS_ENABLED = os.getenv('DISCORD_NOTIFICATIONS_ENABLED', 'false').lower() == 'true'

# For EC2 deployment: Handle domain configuration
if not raw_domain:
    # If DOMAIN is not set, try to get EC2 public IP or hostname
    ec2_public_ip = os.getenv('EC2_PUBLIC_IP', '')
    if ec2_public_ip:
        raw_domain = ec2_public_ip
        print(f"Using EC2_PUBLIC_IP: {ec2_public_ip}")
    else:
        # Fallback - you should set this manually for production
        print("WARNING: No DOMAIN or EC2_PUBLIC_IP set. WebSocket connections may fail.")
        raw_domain = 'localhost'
else:
    print(f"Using DOMAIN from env: {raw_domain}")

# Strip protocols and trailing slashes from DOMAIN for WebSocket URL
DOMAIN = re.sub(r'(^\w+:|^)\/\/|\/+$', '', raw_domain) 
print(f"Final DOMAIN after processing: '{DOMAIN}'")

PORT = int(os.getenv('PORT', 6060))

# EC2 Security: Add allowed origins for CORS if needed
ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', '*').split(',')

# EC2 Health check configuration
HEALTH_CHECK_TOKEN = os.getenv('HEALTH_CHECK_TOKEN', None)  # Optional security token for health checks

# Updated SYSTEM_MESSAGE based on the product concept and AI persona "ルーメン"
# This is the core instruction for the AI's behavior
SYSTEM_MESSAGE = (
    "あなたはLumen Engageシステムのアシスタント、「ルーメン」です。"
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
    "3.  **ヒアリング（最重要フェーズ）:** 時間確認が取れたら、「先ほどは弊社ウェブサイトよりお問い合わせ（または資料ダウンロード等）いただきありがとうございます。差し支えなければ、**現在、貴社ではウェブサイトからのお問い合わせやフォームに、どのようにご対応されているか**、簡単にお伺いしてもよろしいでしょうか？」と最初の質問を投げかけ、フォーム対応状況に関す
るヒアリングを開始する。LP登録のトリガー（お問い合わせ or 資料ダウンロードなど）は会話に含めても良い。\n"
    "4.  **詳細ヒアリング（営業担当として深掘り）:** お客様の発言に対し、**一度に一つの質問のみ**で、さらに深掘りして以下の点を引き出すよう、積極的に質問を組み立てる。\n"
    "    *   ウェブサイトからのお問い合わせ対応は、**主にどなたが担当されているか**？ (例: 営業担当、マーケティング担当、事務担当など)\n"
    "    *   お問い合わせ内容の管理や、お客様への返信には、**どのようなツールやシステムを使っているか**？ (例: メールソフト、スプレッドシート、CMS付属機能、SFA/CRMなど)\n"
    "    *   現在、問い合わせ対応やフォーム運用に関して、**何か課題や手間になっていること**はあるか？\n"
    "    *   （可能であれば）お問い合わせからお客様への最初の返信まで、**平均してどのくらいの時間がかかっているか**？\n"
    "    *   （可能であれば）現在のお問い合わせ対応の体制で、**人員的な余裕はどのくらいあるか**？\n"
    "5.  **最適な次ステップの提案（積極的な意思決定）:** ヒアリングで得られた情報（特に課題や担当者の状況、システム利用状況）を総合的に判断し、お客様にとって最も価値のある次のアクションを「ルーメン」自身が積極的に提案する。提案する際は、Lumen Engageがお客様の課題（例: 対応時間短縮、担当者の負荷軽減、情報の一元管理など）をどのように解決できるか簡潔に言及する
。\n"
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
    "*   お客様の名前を呼ぶ際は、苗字のみを使用してください。（例: 山田様）\n"
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

# Add CORS middleware for EC2 deployment
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

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

    # Initialize conversation tracking
    conversation_start_time = time.time()
    # Generate a fallback ID if call_id is None
    tracking_id = call_id or f"session_{int(conversation_start_time)}"
    
    conversation_data = {
        'call_id': call_id,
        'tracking_id': tracking_id,
        'customer_name': customer_name,
        'company_name': company_name,
        'start_time': conversation_start_time,
        'customer_confirmed': False,
        'current_inquiry_process': None,
        'tools_used': None,
        'challenges': None,
        'next_step': None,
        'scheduled_meeting': None,
        'conversation_events': []
    }
    
    # Store conversation data globally using both call_id and tracking_id
    if call_id:
        CONVERSATION_DATA[call_id] = conversation_data
    CONVERSATION_DATA[tracking_id] = conversation_data
    
    print(f"Initialized conversation tracking with call_id: {call_id}, tracking_id: {tracking_id}")

    print(f"Attempting to connect to OpenAI WebSocket...")
    try:
        async with websockets.connect(
            'wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2025-06-03', # Use the recommended model for Realtime
            additional_headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "OpenAI-Beta": "realtime=v1"
            }
        ) as openai_ws:
            print("Connected to OpenAI WebSocket successfully")
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
                            
                            # Update conversation data with stream_sid and store with stream_sid as key too
                            conversation_data['stream_sid'] = stream_sid
                            CONVERSATION_DATA[stream_sid] = conversation_data
                            
                            # Update all existing entries
                            if call_id and call_id in CONVERSATION_DATA:
                                CONVERSATION_DATA[call_id]['stream_sid'] = stream_sid
                            if tracking_id in CONVERSATION_DATA:
                                CONVERSATION_DATA[tracking_id]['stream_sid'] = stream_sid
                                
                            print(f"Updated conversation data with stream_sid: {stream_sid}")
                            
                        elif data['event'] == 'stop': # Handle call end
                             print(f"Twilio stream {stream_sid} stopped.")
                             
                             # Try to find conversation data using multiple keys
                             found_conversation_data = None
                             used_key = None
                             
                             # Try different keys to find conversation data
                             for key in [call_id, tracking_id, stream_sid]:
                                 if key and key in CONVERSATION_DATA:
                                     found_conversation_data = CONVERSATION_DATA[key]
                                     used_key = key
                                     print(f"Found conversation data using key: {key}")
                                     break
                             
                             if found_conversation_data:
                                 print(f"Processing call completion for key: {used_key}")
                                 end_time = time.time()
                                 duration_seconds = int(end_time - conversation_start_time)
                                 duration_minutes = duration_seconds // 60
                                 duration_remainder = duration_seconds % 60
                                 duration_str = f"{duration_minutes}分{duration_remainder}秒"
                                 
                                 found_conversation_data['end_time'] = end_time
                                 found_conversation_data['duration'] = duration_str
                                 
                                 # Extract conversation summary and send Discord notification
                                 conversation_summary = extract_conversation_summary(found_conversation_data)
                                 
                                 # Prepare call data for Discord notification
                                 discord_call_data = {
                                     'name': customer_name,
                                     'company_name': company_name,
                                     'call_sid': stream_sid or call_id or tracking_id,
                                     'duration': duration_str
                                 }
                                 
                                 print(f"Discord notification settings - Enabled: {DISCORD_NOTIFICATIONS_ENABLED}, Webhook URL configured: {bool(DISCORD_WEBHOOK_URL)}")
                                 print(f"Sending Discord notification with data: {discord_call_data}")
                                 print(f"Conversation summary: {conversation_summary}")
                                 
                                 # Send Discord notification in background
                                 asyncio.create_task(send_discord_notification(discord_call_data, conversation_summary))
                                 
                                 print(f"Call completed. Duration: {duration_str}")
                             else:
                                 print(f"No conversation data found for any key: call_id={call_id}, tracking_id={tracking_id}, stream_sid={stream_sid}")
                                 print(f"Available conversation data keys: {list(CONVERSATION_DATA.keys())}")
                                 
                                 # Send basic notification even without conversation data
                                 basic_call_data = {
                                     'name': customer_name,
                                     'company_name': company_name,
                                     'call_sid': stream_sid or call_id or tracking_id or 'unknown',
                                     'duration': 'unknown'
                                 }
                                 print(f"Sending basic Discord notification: {basic_call_data}")
                                 asyncio.create_task(send_discord_notification(basic_call_data, "会話データが記録されませんでした"))
                             
                             if openai_ws.state == websockets.protocol.State.OPEN:
                                 await openai_ws.close() # Close OpenAI WebSocket
                                 print("Closed OpenAI WebSocket.")

                except WebSocketDisconnect:
                    print("Client disconnected (WebSocketDisconnect).")
                    # Handle disconnect - send notification if call data exists
                    found_conversation_data = None
                    used_key = None
                    
                    # Try different keys to find conversation data
                    for key in [call_id, tracking_id, stream_sid]:
                        if key and key in CONVERSATION_DATA:
                            found_conversation_data = CONVERSATION_DATA[key]
                            used_key = key
                            print(f"Found conversation data on disconnect using key: {key}")
                            break
                    
                    if found_conversation_data:
                        print(f"Processing disconnect for key: {used_key}")
                        end_time = time.time()
                        duration_seconds = int(end_time - conversation_start_time)
                        duration_minutes = duration_seconds // 60
                        duration_remainder = duration_seconds % 60
                        duration_str = f"{duration_minutes}分{duration_remainder}秒"
                        
                        found_conversation_data['end_time'] = end_time
                        found_conversation_data['duration'] = duration_str
                        
                        conversation_summary = extract_conversation_summary(found_conversation_data)
                        discord_call_data = {
                            'name': customer_name,
                            'company_name': company_name,
                            'call_sid': stream_sid or call_id or tracking_id,
                            'duration': duration_str
                        }
                        
                        print(f"Discord notification settings - Enabled: {DISCORD_NOTIFICATIONS_ENABLED}, Webhook URL configured: {bool(DISCORD_WEBHOOK_URL)}")
                        print(f"Sending Discord notification on disconnect: {discord_call_data}")
                        
                        asyncio.create_task(send_discord_notification(discord_call_data, conversation_summary))
                        print(f"Call disconnected. Duration: {duration_str}")
                    else:
                        print(f"No conversation data found for disconnected call: call_id={call_id}, tracking_id={tracking_id}, stream_sid={stream_sid}")
                        print(f"Available conversation data keys: {list(CONVERSATION_DATA.keys())}")
                        
                        # Send basic notification even without conversation data
                        basic_call_data = {
                            'name': customer_name,
                            'company_name': company_name,
                            'call_sid': stream_sid or call_id or tracking_id or 'unknown',
                            'duration': 'unknown'
                        }
                        print(f"Sending basic Discord notification on disconnect: {basic_call_data}")
                        asyncio.create_task(send_discord_notification(basic_call_data, "会話データが記録されませんでした（切断）"))
                    
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
                        
                        # Track conversation events for Discord notification
                        # Try to find the conversation data using multiple keys
                        active_conversation_data = None
                        for key in [call_id, tracking_id, stream_sid]:
                            if key and key in CONVERSATION_DATA:
                                active_conversation_data = CONVERSATION_DATA[key]
                                break
                        
                        if active_conversation_data:
                            # Extract conversation content from response.content.done
                            if response['type'] == 'response.content.done':
                                content = response.get('content', [])
                                for item in content:
                                    if item.get('type') == 'text':
                                        text_content = item.get('text', '')
                                        active_conversation_data['conversation_events'].append({
                                            'type': 'ai_response',
                                            'content': text_content,
                                            'timestamp': time.time()
                                        })
                            
                            # Also extract from response.done events which contain transcript
                            elif response['type'] == 'response.done':
                                response_data = response.get('response', {})
                                output = response_data.get('output', [])
                                for item in output:
                                    if item.get('type') == 'message' and item.get('role') == 'assistant':
                                        content = item.get('content', [])
                                        for content_item in content:
                                            if content_item.get('type') == 'audio':
                                                transcript = content_item.get('transcript', '')
                                                if transcript:
                                                    active_conversation_data['conversation_events'].append({
                                                        'type': 'ai_transcript',
                                                        'content': transcript,
                                                        'timestamp': time.time()
                                                    })
                                                    print(f"Recorded AI transcript: {transcript[:50]}...")
                            
                            # Track user speech from conversation.item.created events
                            elif response['type'] == 'conversation.item.created':
                                item = response.get('item', {})
                                if item.get('type') == 'message' and item.get('role') == 'user':
                                    content = item.get('content', [])
                                    for content_item in content:
                                        if content_item.get('type') == 'input_text':
                                            user_text = content_item.get('text', '')
                                            if user_text:
                                                active_conversation_data['conversation_events'].append({
                                                    'type': 'user_text',
                                                    'content': user_text,
                                                    'timestamp': time.time()
                                                })
                                                print(f"Recorded user text: {user_text[:50]}...")
                                        elif content_item.get('type') == 'input_audio':
                                            # Note: Audio content doesn't contain transcript directly
                                            # We might get transcript from input_audio_buffer.speech_stopped
                                            active_conversation_data['conversation_events'].append({
                                                'type': 'user_audio',
                                                'content': '[音声入力]',
                                                'timestamp': time.time()
                                            })
                                            print("Recorded user audio input")
                            
                            # Track speech detection events
                            elif response['type'] == 'input_audio_buffer.speech_started':
                                active_conversation_data['conversation_events'].append({
                                    'type': 'user_speech_start',
                                    'content': '[ユーザー発話開始]',
                                    'timestamp': time.time()
                                })
                                print("User speech started")
                            
                            elif response['type'] == 'input_audio_buffer.speech_stopped':
                                active_conversation_data['conversation_events'].append({
                                    'type': 'user_speech_end',
                                    'content': '[ユーザー発話終了]',
                                    'timestamp': time.time()
                                })
                                print("User speech stopped")
                            
                            # Track when audio buffer is committed (user finished speaking)
                            elif response['type'] == 'input_audio_buffer.committed':
                                active_conversation_data['conversation_events'].append({
                                    'type': 'user_speech_committed',
                                    'content': '[ユーザー発話確定]',
                                    'timestamp': time.time()
                                })
                                print("User speech committed")
                        
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

    except Exception as e:
        print(f"Failed to connect to OpenAI WebSocket: {e}")
        await websocket.close()
        return


async def send_initial_conversation_item(openai_ws, customer_name: str, company_name: str, current_date_jst: str):
    """Send initial conversation so AI talks first, including customer info and date."""

    # Extract surname from customer_name (simple splitting by space)
    # Note: This is a basic approach and might not work for all naming conventions.
    # A more robust approach might be needed for production.
    surname = customer_name.split(' ')[0] if customer_name else 'お客様' # Use surname or default

    # Construct the initial instruction/context for the AI
    # We provide the customer info and today's date as context for the AI to use in the conversation
    context_message = (
        f"電話が接続されました。お客様は**{company_name}の{customer_name}様**です。ただし、会話中にお客様の名前を呼ぶ際は、**苗字のみ（「{surname}様」のように）**を使用してください。"
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

    # Determine protocol based on whether we're using HTTPS or not
    # For SSL deployments (domain with SSL or ngrok), use https://
    # For EC2 without SSL, use http:// (but this won't work with Twilio in production)
    use_ssl = os.getenv('USE_SSL', 'false').lower() == 'true'
    
    # Check if DOMAIN includes protocol already (for ngrok compatibility)
    if raw_domain.startswith(('http://', 'https://')):
        # Extract protocol from domain
        use_ssl = raw_domain.startswith('https://')
        print(f"Domain includes protocol, extracted SSL setting: {use_ssl}")
    
    # For WebSocket connections: ws:// or wss://
    ws_protocol = "wss" if use_ssl else "ws"
    print(f"WebSocket protocol for TwiML: {ws_protocol}")
    print(f"SSL enabled: {use_ssl}")
    print(f"Port: {PORT}")
    
    # Determine if we should include port in URL
    # Standard ports (80 for ws, 443 for wss) are usually omitted
    # Custom ports are included unless using a reverse proxy
    include_port = True
    
    if PORT == 80 and ws_protocol == "ws":
        include_port = False
        print("Using standard WebSocket port 80, omitting from URL")
    elif PORT == 443 and ws_protocol == "wss":
        include_port = False
        print("Using standard WebSocket Secure port 443, omitting from URL")
    elif use_ssl and PORT == 6060:
        # Check if we're behind a reverse proxy (like Nginx)
        # In production with Nginx, the external port would be 443, not 6060
        behind_proxy = os.getenv('BEHIND_PROXY', 'false').lower() == 'true'
        if behind_proxy:
            include_port = False
            print("Behind reverse proxy, omitting port from URL")
        else:
            print(f"Direct SSL setup, including port {PORT} in URL")
    else:
        print(f"Using custom port {PORT}, including in URL")
    
    # Construct WebSocket Stream URL with query parameters
    # Use WebSocket protocol - wss:// for SSL, ws:// for non-SSL
    if include_port:
        stream_url = f"{ws_protocol}://{DOMAIN}:{PORT}/media-stream"
    else:
        stream_url = f"{ws_protocol}://{DOMAIN}/media-stream"
    
    print(f"Base WebSocket Stream URL: {stream_url}")
    
    # Generate a unique call ID for this call (using call SID later)
    temp_call_id = str(uuid.uuid4())
    query_params = {
        'call_id': temp_call_id
        # Removed other params since they might not be passed by Twilio
    }

    # Encode query parameters with proper handling of special characters
    encoded_query_params = '&'.join(f"{key}={urllib.parse.quote(value)}" for key, value in query_params.items())
    stream_url_with_params = f"{stream_url}?{encoded_query_params}"
    print(f"Stream URL with params: {stream_url_with_params}")
    
    # Properly escape the URL for XML
    escaped_stream_url = html.escape(stream_url_with_params)
    print(f"Escaped Stream URL: {escaped_stream_url}")
    
    # Check URL length and format
    print(f"URL length: {len(escaped_stream_url)} characters")
    if len(escaped_stream_url) > 2048:
        print("WARNING: URL is very long, might cause issues")
    
    # Validate URL format - should be WebSocket (ws:// or wss://) for TwiML Media Streams
    if not escaped_stream_url.startswith(('ws://', 'wss://')):
        print(f"ERROR: Invalid URL protocol in: {escaped_stream_url}")
    
    print(f"WebSocket Stream URL sent to Twilio: {escaped_stream_url}") # Log the exact URL

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
        f'<Response><Connect><Stream url="{escaped_stream_url}" /></Connect></Response>'
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
    
    # Clean up old conversation data
    cleanup_conversation_data()

    return call.sid

def cleanup_call_data_storage():
    """Remove old entries from CALL_DATA_STORAGE based on timestamp."""
    current_time = time.time()
    expired_keys = [key for key, data in CALL_DATA_STORAGE.items() if current_time - data['timestamp'] > CALL_DATA_TIMEOUT_SECONDS]
    for key in expired_keys:
        del CALL_DATA_STORAGE[key]
        print(f"Cleaned up expired call data for key {key}")

def cleanup_conversation_data():
    """Remove old entries from CONVERSATION_DATA based on timestamp."""
    current_time = time.time()
    expired_keys = []
    for key, data in CONVERSATION_DATA.items():
        # Clean up conversations older than 1 hour
        if current_time - data.get('start_time', 0) > 3600:
            expired_keys.append(key)
    
    for key in expired_keys:
        del CONVERSATION_DATA[key]
        print(f"Cleaned up expired conversation data for key {key}")

async def log_call_sid(call_sid):
    """Log the call SID."""
    print(f"Call started with SID: {call_sid}")
    # In a real application, you would log this to a database, monitoring system, etc.

@app.post('/test-discord-notification')
async def test_discord_notification():
    """Test endpoint to verify Discord notification functionality."""
    if not DISCORD_NOTIFICATIONS_ENABLED:
        raise HTTPException(status_code=400, detail="Discord notifications are disabled")
    
    if not DISCORD_WEBHOOK_URL:
        raise HTTPException(status_code=400, detail="Discord webhook URL not configured")
    
    # Create test data
    test_call_data = {
        'name': 'テスト太郎',
        'company_name': 'テスト株式会社',
        'call_sid': 'TEST_CALL_SID_123',
        'duration': '3分45秒'
    }
    
    test_conversation_summary = (
        "✅ お客様情報確認済み\n"
        "📋 現在の問い合わせ対応: メールで手動対応中\n"
        "🛠️ 使用ツール: Gmail、Excelスプレッドシート\n"
        "⚠️ 課題: 返信に時間がかかる、情報管理が大変\n"
        "➡️ 次のステップ: 営業担当との面談を予定\n"
        "📅 面談予定: 来週火曜日 14:00-15:00\n"
        "⏱️ 通話時間: 3分45秒"
    )
    
    try:
        await send_discord_notification(test_call_data, test_conversation_summary)
        return {"success": True, "message": "Test Discord notification sent successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send test notification: {str(e)}")

@app.get('/health')
async def health_check(authorization: str = Header(None)):
    """Health check endpoint for EC2 load balancer."""
    # Optional security check for health endpoint
    if HEALTH_CHECK_TOKEN and authorization != f"Bearer {HEALTH_CHECK_TOKEN}":
        # Still return 200 for basic health checks, but with limited info
        return {"status": "ok"}
    
    # Detailed health check
    health_status = {
        "status": "healthy",
        "service": "Lumen Engage AI Voice Assistant",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "1.0.0",
        "environment": {
            "domain": DOMAIN,
            "port": PORT,
            "has_twilio_config": bool(TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN),
            "has_openai_config": bool(OPENAI_API_KEY),
            "discord_notifications_enabled": DISCORD_NOTIFICATIONS_ENABLED,
            "has_discord_webhook": bool(DISCORD_WEBHOOK_URL)
        }
    }
    
    return health_status

async def send_discord_notification(call_data: dict, conversation_summary: str = None):
    """Send a notification to Discord when a call is completed."""
    print(f"send_discord_notification called with call_data: {call_data}")
    print(f"DISCORD_NOTIFICATIONS_ENABLED: {DISCORD_NOTIFICATIONS_ENABLED}")
    print(f"DISCORD_WEBHOOK_URL configured: {bool(DISCORD_WEBHOOK_URL)}")
    
    if not DISCORD_NOTIFICATIONS_ENABLED or not DISCORD_WEBHOOK_URL:
        print("Discord notifications are disabled or webhook URL not configured")
        return
    
    try:
        print("Preparing Discord notification...")
        # Get current time in JST
        jst = pytz.timezone('Asia/Tokyo')
        current_time_jst = datetime.now(jst).strftime('%Y年%m月%d日 %H:%M:%S JST')
        
        # Prepare the Discord embed
        embed = {
            "title": "🔔 通話完了通知",
            "color": 0x00ff00,  # Green color
            "timestamp": datetime.now(timezone.utc).isoformat(),  # Fix deprecation warning
            "fields": [
                {
                    "name": "👤 お客様情報",
                    "value": f"**名前:** {call_data.get('name', 'N/A')}\n**会社名:** {call_data.get('company_name', 'N/A')}",
                    "inline": False
                },
                {
                    "name": "📞 通話情報",
                    "value": f"**通話ID:** {call_data.get('call_sid', 'N/A')}\n**完了時刻:** {current_time_jst}",
                    "inline": False
                }
            ]
        }
        
        # Add conversation summary if available
        if conversation_summary:
            # Split long summaries into multiple fields if needed (Discord has 1024 char limit per field)
            if len(conversation_summary) > 1024:
                # Split into chunks
                chunks = [conversation_summary[i:i+1024] for i in range(0, len(conversation_summary), 1024)]
                for i, chunk in enumerate(chunks):
                    field_name = f"💬 会話内容 ({i+1}/{len(chunks)})" if len(chunks) > 1 else "💬 会話内容"
                    embed["fields"].append({
                        "name": field_name,
                        "value": chunk,
                        "inline": False
                    })
            else:
                embed["fields"].append({
                    "name": "💬 会話内容",
                    "value": conversation_summary,
                    "inline": False
                })
        else:
            embed["fields"].append({
                "name": "💬 会話内容",
                "value": "会話の詳細は記録されませんでした",
                "inline": False
            })
        
        # Prepare the Discord webhook payload
        payload = {
            "username": "Lumen Engage AI Assistant",
            "avatar_url": "https://cdn.discordapp.com/attachments/123456789/avatar.png",  # Optional: Add your bot avatar URL
            "embeds": [embed]
        }
        
        print(f"Discord payload prepared: {json.dumps(payload, indent=2, ensure_ascii=False)}")
        print(f"Sending to webhook URL: {DISCORD_WEBHOOK_URL[:50]}...")
        
        # Send the notification
        async with aiohttp.ClientSession() as session:
            async with session.post(DISCORD_WEBHOOK_URL, json=payload) as response:
                response_text = await response.text()
                print(f"Discord API response status: {response.status}")
                print(f"Discord API response text: {response_text}")
                
                if response.status == 204:
                    print("Discord notification sent successfully")
                else:
                    print(f"Failed to send Discord notification: {response.status} - {response_text}")
                    
    except Exception as e:
        print(f"Error sending Discord notification: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

def extract_conversation_summary(conversation_data: dict) -> str:
    """Extract and format conversation summary from collected data."""
    if not conversation_data:
        return "会話データが記録されませんでした"
    
    # Get all conversation events and format them chronologically
    events = conversation_data.get('conversation_events', [])
    if not events:
        return "会話イベントが記録されませんでした"
    
    # Sort events by timestamp
    sorted_events = sorted(events, key=lambda x: x.get('timestamp', 0))
    
    # Filter out system initialization messages (they usually contain system prompts)
    filtered_events = []
    for event in sorted_events:
        content = event.get('content', '')
        event_type = event.get('type', '')
        
        # Skip system initialization messages that contain system prompts
        if (event_type == 'user_text' and 
            ('電話が接続されました' in content or 
             'システムプロンプト' in content or 
             'あなたの最初の発話は短く' in content or
             len(content) > 500)):  # Very long messages are likely system prompts
            print(f"Filtering out system message: {content[:100]}...")
            continue
        
        filtered_events.append(event)
    
    # Format the conversation transcript
    transcript_parts = []
    transcript_parts.append(f"📞 通話時間: {conversation_data.get('duration', 'unknown')}")
    transcript_parts.append(f"👤 お客様: {conversation_data.get('customer_name', 'N/A')} ({conversation_data.get('company_name', 'N/A')})")
    transcript_parts.append("")
    transcript_parts.append("💬 会話の流れ:")
    transcript_parts.append("")
    
    # Group events and format them nicely
    for i, event in enumerate(filtered_events):
        event_type = event.get('type', 'unknown')
        content = event.get('content', '')
        
        # Format different types of events
        if event_type == 'ai_transcript':
            transcript_parts.append(f"🤖 **AI**: {content}")
        elif event_type == 'ai_response':
            transcript_parts.append(f"🤖 **AI**: {content}")
        elif event_type == 'user_text':
            transcript_parts.append(f"👤 **ユーザー**: {content}")
        elif event_type == 'user_speech_start':
            transcript_parts.append(f"🎤 {content}")
        elif event_type == 'user_speech_end':
            transcript_parts.append(f"🔇 {content}")
        elif event_type == 'user_speech_committed':
            transcript_parts.append(f"✅ {content}")
        elif event_type == 'user_audio':
            transcript_parts.append(f"🎵 {content}")
        else:
            transcript_parts.append(f"📝 **{event_type}**: {content}")
        
        # Add a small separator between different speakers
        if i < len(filtered_events) - 1:
            next_event = filtered_events[i + 1]
            current_speaker = 'ai' if event_type in ['ai_transcript', 'ai_response'] else 'user'
            next_speaker = 'ai' if next_event.get('type') in ['ai_transcript', 'ai_response'] else 'user'
            
            # Add separator when speaker changes
            if current_speaker != next_speaker and event_type in ['ai_transcript', 'ai_response', 'user_text']:
                transcript_parts.append("---")
    
    full_transcript = "\n".join(transcript_parts)
    
    # If the transcript is too long, truncate it but keep the structure
    if len(full_transcript) > 4000:  # Leave some room for Discord's 6000 char limit
        lines = transcript_parts
        truncated_lines = []
        current_length = 0
        
        # Keep header information
        for line in lines[:5]:  # Keep first 5 lines (header info)
            truncated_lines.append(line)
            current_length += len(line) + 1  # +1 for newline
        
        # Add as many conversation lines as possible
        for line in lines[5:]:
            if current_length + len(line) + 50 > 4000:  # Leave room for truncation message
                truncated_lines.append("...")
                truncated_lines.append("📝 *会話が長いため、一部省略されています*")
                break
            truncated_lines.append(line)
            current_length += len(line) + 1
        
        return "\n".join(truncated_lines)
    
    return full_transcript

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
        '  POST /test-discord-notification - Test Discord notifications\n'
        '\n'
        'Discord Configuration:\n'
        f'  Notifications Enabled: {DISCORD_NOTIFICATIONS_ENABLED}\n'
        f'  Webhook URL Configured: {bool(DISCORD_WEBHOOK_URL)}\n'
        f'  Webhook URL (first 50 chars): {DISCORD_WEBHOOK_URL[:50] if DISCORD_WEBHOOK_URL else "Not configured"}\n'
    )
    
    uvicorn.run(app, host="0.0.0.0", port=PORT)
