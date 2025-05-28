#!/bin/bash

# SSL Setup Script for EC2 with Nginx and Let's Encrypt
# Run this on your EC2 instance

set -e

# Variables - Update these!
DOMAIN_NAME="your-domain.com"  # Replace with your actual domain
EMAIL="your-email@example.com"  # Replace with your email for Let's Encrypt

echo "🔒 Setting up SSL for Lumen Engage AI Voice Assistant"
echo "Domain: $DOMAIN_NAME"
echo "Email: $EMAIL"

# Check if running as root
if [[ $EUID -eq 0 ]]; then
   echo "This script should not be run as root for security. Run as ubuntu/ec2-user."
   exit 1
fi

# Update system
echo "📦 Updating system packages..."
sudo apt update -y
sudo apt upgrade -y

# Install Nginx
echo "🌐 Installing Nginx..."
sudo apt install -y nginx

# Install Certbot for Let's Encrypt
echo "🔐 Installing Certbot..."
sudo apt install -y certbot python3-certbot-nginx

# Create initial HTTP-only Nginx configuration
echo "⚙️ Creating initial HTTP Nginx configuration..."
sudo tee /etc/nginx/sites-available/lumen-engage << EOF
server {
    listen 80;
    server_name $DOMAIN_NAME;

    # Proxy regular HTTP requests to FastAPI
    location / {
        proxy_pass http://127.0.0.1:6060;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }

    # Proxy WebSocket connections
    location /media-stream {
        proxy_pass http://127.0.0.1:6060;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_read_timeout 86400;
    }

    # Let's Encrypt challenge location
    location /.well-known/acme-challenge/ {
        root /var/www/html;
    }
}
EOF

# Enable the site
sudo ln -sf /etc/nginx/sites-available/lumen-engage /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default

# Create web root for Let's Encrypt
sudo mkdir -p /var/www/html

# Test Nginx configuration
echo "🧪 Testing initial Nginx configuration..."
sudo nginx -t

# Start and enable Nginx
echo "🚀 Starting Nginx..."
sudo systemctl restart nginx
sudo systemctl enable nginx

# Get SSL certificate
echo "🔐 Obtaining SSL certificate..."
sudo certbot --nginx -d $DOMAIN_NAME --email $EMAIL --agree-tos --non-interactive

# Test certificate renewal
echo "🔄 Testing certificate renewal..."
sudo certbot renew --dry-run

echo "✅ SSL setup complete!"
echo ""
echo "Nginx has been configured with SSL and will automatically redirect HTTP to HTTPS."
echo ""
echo "Next steps:"
echo "1. Update your .env file to use HTTPS:"
echo "   DOMAIN=$DOMAIN_NAME"
echo "   USE_SSL=true"
echo "   # Remove EC2_PUBLIC_IP line"
echo ""
echo "2. Restart your application:"
echo "   cd ~/outbound-calling-speech-assistant-openai-realtime-api-python"
echo "   source venv/bin/activate"
echo "   python3 web2.py"
echo ""
echo "3. Test your setup:"
echo "   curl https://$DOMAIN_NAME/health"
echo ""
echo "4. Update your Security Group to allow ports 80 and 443:"
echo "   - Port 80 (HTTP) - for Let's Encrypt renewals"
echo "   - Port 443 (HTTPS) - for Twilio webhooks"
echo "   - You can close port 6060 since Nginx will proxy requests" 