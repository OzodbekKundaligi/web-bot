# server.py - PostgreSQL version
import os
import json
import logging
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request, session, redirect, url_for
from flask_cors import CORS
from functools import wraps
import threading
import time
from dotenv import load_dotenv

from db import check_database_connection

# Load environment variables
load_dotenv()

# Database import
try:
    from db import (
        init_db,
        get_user, save_user, update_user_field,
        create_startup, get_startup, get_startups_by_owner,
        get_pending_startups, get_active_startups, update_startup_status,
        get_statistics, get_all_users, get_recent_users, get_recent_startups,
        get_completed_startups, get_rejected_startups, get_startup_members,
        save_broadcast_message, log_admin_action
    )
    DB_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ Database import xatosi: {e}")
    DB_AVAILABLE = False
    # Database bo'lmasa xatolik qaytarish
    def database_error():
        return jsonify({'success': False, 'error': 'Database ulanmagan'}), 500

# Bot import
try:
    import telebot
    BOT_TOKEN = os.getenv('BOT_TOKEN')
    if BOT_TOKEN:
        bot = telebot.TeleBot(BOT_TOKEN)
        BOT_AVAILABLE = True
    else:
        print("⚠️ BOT_TOKEN environment variable o'rnatilmagan!")
        BOT_AVAILABLE = False
except ImportError as e:
    print(f"⚠️ Telegram bot import xatosi: {e}")
    BOT_AVAILABLE = False

app = Flask(__name__, template_folder='templates', static_folder='static')

# Environment variables
SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret-key-' + str(os.urandom(24).hex()))
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'admin123')
ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', 'admin@garajhub.uz')
CHANNEL_USERNAME = os.environ.get('CHANNEL_USERNAME', '@GarajHub_uz')

app.secret_key = SECRET_KEY
app.config['SESSION_TYPE'] = 'filesystem'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=24)

# CORS sozlamalari
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Logger sozlash
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Adminlar ro'yxati
ADMINS = {
    'admin': {
        'password': ADMIN_PASSWORD,
        'full_name': 'Super Admin',
        'email': ADMIN_EMAIL,
        'role': 'superadmin'
    }
}

# Login talab qiluvchi decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'admin_logged_in' not in session:
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated_function

# Botni ishga tushirish funksiyasi
def start_bot():
    if BOT_AVAILABLE:
        try:
            print("🤖 Telegram bot ishga tushmoqda...")
            bot.remove_webhook()
            time.sleep(1)
            bot.infinity_polling(timeout=60, long_polling_timeout=60)
        except Exception as e:
            print(f"Bot xatosi: {e}")
            time.sleep(5)
            start_bot()
    else:
        print("⚠️ Bot mavjud emas")

# ==================== ROUTES ====================

@app.route('/')
def index():
    """Asosiy sahifa"""
    return render_template('index.html')

@app.route('/api/login', methods=['POST'])
def login():
    """Admin login API"""
    try:
        data = request.json
        username = data.get('username')
        password = data.get('password')
        
        if not username or not password:
            return jsonify({'error': 'Username va password kiriting'}), 400
        
        admin = ADMINS.get(username)
        if admin and admin['password'] == password:
            session['admin_logged_in'] = True
            session['admin_username'] = username
            session['admin_role'] = admin['role']
            session['admin_name'] = admin['full_name']
            session.permanent = True
            
            # Log admin login
            log_admin_action(username, 'login', {
                'ip': request.remote_addr,
                'user_agent': request.headers.get('User-Agent')
            }, request.remote_addr)
            
            logger.info(f"Admin kirildi: {username}")
            return jsonify({
                'success': True,
                'user': {
                    'username': username,
                    'full_name': admin['full_name'],
                    'email': admin['email'],
                    'role': admin['role']
                }
            })
        else:
            return jsonify({'error': 'Noto\'g\'ri login yoki parol'}), 401
    except Exception as e:
        logger.error(f"Login error: {str(e)}")
        return jsonify({'error': 'Server xatosi'}), 500

@app.route('/api/logout', methods=['POST'])
def logout():
    """Logout API"""
    session.clear()
    return jsonify({'success': True})

@app.route('/api/check_auth')
def check_auth():
    """Auth tekshirish"""
    if 'admin_logged_in' in session:
        return jsonify({
            'authenticated': True,
            'user': {
                'username': session.get('admin_username'),
                'full_name': session.get('admin_name'),
                'role': session.get('admin_role')
            }
        })
    return jsonify({'authenticated': False})

@app.route('/api/statistics')
@login_required
def get_statistics_data():
    """Statistika ma'lumotlari"""
    try:
        if not DB_AVAILABLE:
            return jsonify({'success': False, 'error': 'Database ulanmagan'}), 500
        
        stats = get_statistics()
        
        # Activity rate calculation
        today = datetime.now().strftime('%Y-%m-%d')
        recent_users = get_recent_users(1000)
        new_today = sum(1 for user in recent_users if user.get('joined_at', '').startswith(today))
        
        week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        active_last_week = sum(1 for user in recent_users if user.get('joined_at', '') >= week_ago)
        total_users = stats.get('total_users', 1)
        activity_rate = round((active_last_week / total_users) * 100) if total_users > 0 else 0
        
        # Log admin action
        log_admin_action(session.get('admin_username'), 'view_statistics')
        
        return jsonify({
            'success': True,
            'data': {
                'total_users': stats.get('total_users', 0),
                'total_startups': stats.get('total_startups', 0),
                'active_startups': stats.get('active_startups', 0),
                'pending_startups': stats.get('pending_startups', 0),
                'completed_startups': stats.get('completed_startups', 0),
                'rejected_startups': stats.get('rejected_startups', 0),
                'new_today': new_today,
                'activity_rate': min(activity_rate, 100)
            }
        })
    except Exception as e:
        logger.error(f"Statistics error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/users')
@login_required
def get_users():
    """Foydalanuvchilar ro'yxati"""
    try:
        if not DB_AVAILABLE:
            return jsonify({'success': False, 'error': 'Database ulanmagan'}), 500
        
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 20))
        search = request.args.get('search', '')
        
        # Database dan foydalanuvchilarni olish
        users = get_recent_users(10000)
        
        # Filtrlash
        if search:
            filtered_users = []
            for user in users:
                first_name = user.get('first_name', '').lower()
                last_name = user.get('last_name', '').lower()
                phone = user.get('phone', '').lower()
                search_lower = search.lower()
                
                if (search_lower in first_name or 
                    search_lower in last_name or 
                    search_lower in phone or
                    search_lower in str(user.get('user_id', '')).lower()):
                    filtered_users.append(user)
            users = filtered_users
        
        # Tartiblash
        users.sort(key=lambda x: x.get('joined_at', ''), reverse=True)
        
        # Pagination
        total = len(users)
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_users = users[start_idx:end_idx]
        
        # Formatlash
        formatted_users = []
        for user in paginated_users:
            formatted_users.append({
                'id': user.get('user_id', ''),
                'user_id': user.get('user_id', ''),
                'first_name': user.get('first_name', 'Noma\'lum'),
                'last_name': user.get('last_name', ''),
                'username': user.get('username', ''),
                'phone': user.get('phone', 'Telefon kiritilmagan'),
                'bio': user.get('bio', ''),
                'joined_at': user.get('joined_at', 'Noma\'lum'),
                'status': user.get('status', 'active')
            })
        
        # Log admin action
        log_admin_action(session.get('admin_username'), 'view_users', {
            'page': page,
            'search': search
        })
        
        return jsonify({
            'success': True,
            'data': formatted_users,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'total_pages': (total + per_page - 1) // per_page
            }
        })
    except Exception as e:
        logger.error(f"Users error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'data': []
        }), 500

@app.route('/api/startups')
@login_required
def get_startups_list():
    """Startaplar ro'yxati"""
    try:
        if not DB_AVAILABLE:
            return jsonify({'success': False, 'error': 'Database ulanmagan'}), 500
        
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 20))
        search = request.args.get('search', '')
        status = request.args.get('status', 'all')
        
        # Database dan startaplarni olish
        startups_data = []
        total = 0
        
        if status == 'active':
            startups_data, total = get_active_startups(page, per_page)
        elif status == 'pending':
            startups_data, total = get_pending_startups(page, per_page)
        elif status == 'completed':
            startups_data, total = get_completed_startups(page, per_page)
        elif status == 'rejected':
            startups_data, total = get_rejected_startups(page, per_page)
        else:
            # Barcha startaplar
            all_startups = []
            
            active, active_total = get_active_startups(1, 1000)
            pending, pending_total = get_pending_startups(1, 1000)
            completed, completed_total = get_completed_startups(1, 1000)
            rejected, rejected_total = get_rejected_startups(1, 1000)
            
            all_startups = active + pending + completed + rejected
            total = len(all_startups)
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page
            startups_data = all_startups[start_idx:end_idx]
        
        # Qidiruv bo'lsa
        if search and startups_data:
            filtered_startups = []
            for startup in startups_data:
                if search.lower() in startup.get('name', '').lower():
                    filtered_startups.append(startup)
            startups_data = filtered_startups
            total = len(startups_data)
        
        # Formatlash
        formatted_startups = []
        for startup in startups_data:
            # A'zolar soni
            try:
                members, member_count = get_startup_members(startup.get('id', ''), 1, 1000)
            except:
                member_count = 0
            
            # Status matni
            status_texts = {
                'pending': '⏳ Kutilmoqda',
                'active': '▶️ Faol',
                'completed': '✅ Yakunlangan',
                'rejected': '❌ Rad etilgan'
            }
            
            formatted_startups.append({
                'id': startup.get('id', ''),
                'name': startup.get('name', 'Noma\'lum'),
                'owner_name': f"{startup.get('owner_first_name', '')} {startup.get('owner_last_name', '')}".strip(),
                'owner_id': startup.get('owner_id', ''),
                'status': startup.get('status', 'pending'),
                'status_text': status_texts.get(startup.get('status', 'pending'), startup.get('status', 'pending')),
                'created_at': startup.get('created_at', 'Noma\'lum'),
                'description': startup.get('description', ''),
                'member_count': member_count
            })
        
        total_pages = (total + per_page - 1) // per_page if total > 0 else 1
        
        # Log admin action
        log_admin_action(session.get('admin_username'), 'view_startups', {
            'page': page,
            'status': status,
            'search': search
        })
        
        return jsonify({
            'success': True,
            'data': formatted_startups,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'total_pages': total_pages
            }
        })
    except Exception as e:
        logger.error(f"Startups error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/startup/<int:startup_id>', methods=['GET'])
@login_required
def get_startup_details(startup_id):
    """Startap tafsilotlari"""
    try:
        if not DB_AVAILABLE:
            return jsonify({'success': False, 'error': 'Database ulanmagan'}), 500
        
        startup = get_startup(startup_id)
        if not startup:
            return jsonify({'success': False, 'error': 'Startap topilmadi'}), 404
        
        # Status matni
        status_texts = {
            'pending': '⏳ Kutilmoqda',
            'active': '▶️ Faol',
            'completed': '✅ Yakunlangan',
            'rejected': '❌ Rad etilgan'
        }
        
        # A'zolar soni
        try:
            members, member_count = get_startup_members(startup_id, 1, 1000)
        except:
            member_count = 0
            members = []
        
        # Owner info
        owner_info = None
        if startup.get('owner_id'):
            owner_info = {
                'id': startup.get('owner_id'),
                'first_name': startup.get('owner_first_name', ''),
                'last_name': startup.get('owner_last_name', ''),
                'phone': startup.get('owner_phone', ''),
                'username': startup.get('owner_username', '')
            }
        
        # Log admin action
        log_admin_action(session.get('admin_username'), 'view_startup_details', {
            'startup_id': startup_id,
            'startup_name': startup.get('name')
        })
        
        return jsonify({
            'success': True,
            'data': {
                'id': startup.get('id', ''),
                'name': startup.get('name', ''),
                'description': startup.get('description', ''),
                'status': startup.get('status', ''),
                'status_text': status_texts.get(startup.get('status'), startup.get('status')),
                'created_at': startup.get('created_at'),
                'started_at': startup.get('started_at'),
                'ended_at': startup.get('ended_at'),
                'results': startup.get('results', ''),
                'group_link': startup.get('group_link', ''),
                'logo': startup.get('logo', ''),
                'owner': owner_info,
                'member_count': member_count
            }
        })
    except Exception as e:
        logger.error(f"Startup details error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/startup/<int:startup_id>/approve', methods=['POST'])
@login_required
def approve_startup(startup_id):
    """Startapni tasdiqlash"""
    try:
        if not DB_AVAILABLE:
            return jsonify({'success': False, 'error': 'Database ulanmagan'}), 500
        
        success = update_startup_status(startup_id, 'active')
        
        if success:
            logger.info(f"Startup approved: {startup_id}")
            
            # Log admin action
            log_admin_action(session.get('admin_username'), 'approve_startup', {
                'startup_id': startup_id
            })
            
            # Bot orqali xabar yuborish
            if BOT_AVAILABLE:
                try:
                    startup = get_startup(startup_id)
                    if startup and startup.get('owner_id'):
                        bot.send_message(
                            startup['owner_id'],
                            f"🎉 Tabriklaymiz! Sizning '{startup['name']}' startupingiz tasdiqlandi!"
                        )
                except Exception as e:
                    logger.error(f"Bot orqali xabar yuborishda xato: {e}")
            
            return jsonify({'success': True, 'message': 'Startap tasdiqlandi'})
        else:
            return jsonify({'success': False, 'error': 'Startap topilmadi yoki yangilanmadi'}), 404
    except Exception as e:
        logger.error(f"Approve startup error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/startup/<int:startup_id>/reject', methods=['POST'])
@login_required
def reject_startup(startup_id):
    """Startapni rad etish"""
    try:
        if not DB_AVAILABLE:
            return jsonify({'success': False, 'error': 'Database ulanmagan'}), 500
        
        success = update_startup_status(startup_id, 'rejected')
        
        if success:
            logger.info(f"Startup rejected: {startup_id}")
            
            # Log admin action
            log_admin_action(session.get('admin_username'), 'reject_startup', {
                'startup_id': startup_id
            })
            
            # Bot orqali xabar yuborish
            if BOT_AVAILABLE:
                try:
                    startup = get_startup(startup_id)
                    if startup and startup.get('owner_id'):
                        bot.send_message(
                            startup['owner_id'],
                            f"❌ Sizning '{startup['name']}' startupingiz rad etildi."
                        )
                except Exception as e:
                    logger.error(f"Bot orqali xabar yuborishda xato: {e}")
            
            return jsonify({'success': True, 'message': 'Startap rad etildi'})
        else:
            return jsonify({'success': False, 'error': 'Startap topilmadi yoki yangilanmadi'}), 404
    except Exception as e:
        logger.error(f"Reject startup error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/broadcast', methods=['POST'])
@login_required
def broadcast_message():
    """Xabar yuborish"""
    try:
        data = request.json
        message = data.get('message')
        recipient_type = data.get('recipient_type', 'all')
        
        if not message:
            return jsonify({'success': False, 'error': 'Xabar matni kiritilmagan'}), 400
        
        # Bot orqali xabar yuborish
        sent_count = 0
        failed_count = 0
        
        if BOT_AVAILABLE and DB_AVAILABLE:
            try:
                users = get_all_users()
                total_users = len(users)
                
                for user_id in users:
                    try:
                        bot.send_message(user_id, f"📢 Admin xabari:\n\n{message}")
                        sent_count += 1
                        time.sleep(0.05)  # Flood dan qochish
                    except Exception as e:
                        failed_count += 1
                        logger.error(f"Foydalanuvchiga xabar yuborishda xato {user_id}: {e}")
            except Exception as e:
                logger.error(f"Xabar yuborishda xato: {e}")
        
        # Xabar ma'lumotlarini saqlash
        message_id = save_broadcast_message(
            message=message,
            sent_by=session.get('admin_username'),
            sent_count=sent_count,
            failed_count=failed_count,
            recipient_type=recipient_type
        )
        
        # Log admin action
        log_admin_action(session.get('admin_username'), 'send_broadcast', {
            'message_length': len(message),
            'sent_count': sent_count,
            'failed_count': failed_count,
            'recipient_type': recipient_type
        })
        
        logger.info(f"Broadcast message: {message[:50]}... (sent: {sent_count}, failed: {failed_count})")
        
        return jsonify({
            'success': True,
            'message': 'Xabar yuborildi',
            'data': {
                'id': message_id,
                'message': message,
                'recipient_type': recipient_type,
                'sent_at': datetime.now().isoformat(),
                'sent_by': session.get('admin_username'),
                'sent_count': sent_count,
                'failed_count': failed_count,
                'total_users': sent_count + failed_count
            }
        })
    except Exception as e:
        logger.error(f"Broadcast error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/analytics/user-growth')
@login_required
def get_user_growth():
    """Foydalanuvchi o'sishi uchun analytics"""
    try:
        if not DB_AVAILABLE:
            return jsonify({'success': False, 'error': 'Database ulanmagan'}), 500
        
        period = request.args.get('period', 'month')
        
        # Oxirgi 30 kun uchun ma'lumot
        now = datetime.now()
        data = []
        labels = []
        
        # Oxirgi 30 kun
        for i in range(29, -1, -1):
            date = now - timedelta(days=i)
            date_str = date.strftime('%Y-%m-%d')
            
            # Database dan shu kundagi foydalanuvchilarni olish
            users = get_recent_users(10000)
            day_users = [u for u in users if u.get('joined_at', '').startswith(date_str)]
            
            labels.append(date.strftime('%d.%m'))
            data.append({
                'date': date_str,
                'new_users': len(day_users),
                'total_users': len([u for u in users if u.get('joined_at', '') <= date_str])
            })
        
        # Log admin action
        log_admin_action(session.get('admin_username'), 'view_analytics', {
            'period': period
        })
        
        return jsonify({
            'success': True,
            'data': {
                'labels': labels,
                'datasets': [
                    {
                        'label': 'Yangi foydalanuvchilar',
                        'data': [d['new_users'] for d in data],
                        'borderColor': '#000000',
                        'backgroundColor': 'rgba(0, 0, 0, 0.1)',
                        'tension': 0.4
                    }
                ]
            }
        })
    except Exception as e:
        logger.error(f"Analytics error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/analytics/startup-distribution')
@login_required
def get_startup_distribution():
    """Startap taqsimoti"""
    try:
        if not DB_AVAILABLE:
            return jsonify({'success': False, 'error': 'Database ulanmagan'}), 500
        
        stats = get_statistics()
        
        data = {
            'labels': ['Faol', 'Kutilayotgan', 'Yakunlangan', 'Rad etilgan'],
            'datasets': [{
                'data': [
                    stats.get('active_startups', 0),
                    stats.get('pending_startups', 0),
                    stats.get('completed_startups', 0),
                    stats.get('rejected_startups', 0)
                ],
                'backgroundColor': [
                    '#000000',
                    '#666666',
                    '#999999',
                    '#CCCCCC'
                ],
                'borderColor': '#ffffff',
                'borderWidth': 2
            }]
        }
        
        return jsonify({
            'success': True,
            'data': data,
            'total': stats.get('total_startups', 0)
        })
    except Exception as e:
        logger.error(f"Distribution error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/settings', methods=['GET', 'POST'])
@login_required
def settings():
    """Sozlamalar"""
    try:
        if request.method == 'GET':
            # Real sozlamalar
            bot_status = 'online' if BOT_AVAILABLE else 'offline'
            bot_username = ''
            
            if BOT_AVAILABLE:
                try:
                    bot_info = bot.get_me()
                    bot_username = bot_info.username if hasattr(bot_info, 'username') else 'Noma\'lum'
                except:
                    bot_username = 'Noma\'lum'
            
            return jsonify({
                'success': True,
                'data': {
                    'site_name': 'GarajHub',
                    'admin_email': ADMIN_EMAIL,
                    'timezone': 'Asia/Tashkent',
                    'bot_token': os.environ.get('BOT_TOKEN', ''),
                    'bot_username': bot_username,
                    'channel_username': CHANNEL_USERNAME,
                    'bot_status': bot_status,
                    'database_status': 'online' if DB_AVAILABLE else 'offline',
                    'server_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            })
        else:
            # Sozlamalarni yangilash
            data = request.json
            logger.info(f"Settings update attempted: {data}")
            
            # Log admin action
            log_admin_action(session.get('admin_username'), 'update_settings', {
                'settings': data
            })
            
            # Hozircha faqat log qilamiz, keyinchalik database ga saqlash mumkin
            return jsonify({
                'success': True, 
                'message': 'Sozlamalar saqlash hozircha amalga oshirilmagan',
                'note': 'Kelajakda database ga saqlanadi'
            })
    except Exception as e:
        logger.error(f"Settings error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/admins')
@login_required
def get_admins():
    """Adminlar ro'yxati"""
    try:
        # Faqat haqiqiy adminlar
        admins_list = []
        for username, admin_data in ADMINS.items():
            admins_list.append({
                'username': username,
                'full_name': admin_data['full_name'],
                'email': admin_data['email'],
                'role': admin_data['role'],
                'last_login': datetime.now().isoformat()
            })
        
        return jsonify({
            'success': True,
            'data': admins_list
        })
    except Exception as e:
        logger.error(f"Admins error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/backups')
@login_required
def get_backups():
    """Backup ma'lumotlari"""
    try:
        # Hozircha bo'sh, keyinchalik amalga oshiriladi
        backups = []
        
        return jsonify({
            'success': True,
            'data': backups,
            'note': 'Backup funksiyasi keyinchalik qo\'shiladi'
        })
    except Exception as e:
        logger.error(f"Backups error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/dashboard/overview')
@login_required
def dashboard_overview():
    """Dashboard uchun umumiy ma'lumotlar"""
    try:
        if not DB_AVAILABLE:
            return jsonify({'success': False, 'error': 'Database ulanmagan'}), 500
        
        stats = get_statistics()
        
        # So'nggi startaplar
        recent_startups = get_recent_startups(5)
        
        # So'nggi foydalanuvchilar
        recent_users = get_recent_users(5)
        
        return jsonify({
            'success': True,
            'data': {
                'stats': {
                    'total_users': stats.get('total_users', 0),
                    'total_startups': stats.get('total_startups', 0),
                    'active_startups': stats.get('active_startups', 0),
                    'pending_startups': stats.get('pending_startups', 0)
                },
                'recent_startups': [
                    {
                        'id': s.get('id'),
                        'name': s.get('name'),
                        'owner': f"{s.get('owner_first_name', '')} {s.get('owner_last_name', '')}".strip(),
                        'status': s.get('status'),
                        'created_at': s.get('created_at')
                    }
                    for s in recent_startups
                ],
                'recent_users': [
                    {
                        'id': u.get('user_id'),
                        'name': f"{u.get('first_name', '')} {u.get('last_name', '')}".strip(),
                        'username': u.get('username'),
                        'joined_at': u.get('joined_at')
                    }
                    for u in recent_users
                ]
            }
        })
    except Exception as e:
        logger.error(f"Dashboard error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ==================== ERROR HANDLERS ====================

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Sahifa topilmadi'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Ichki server xatosi'}), 500

# ==================== HEALTH CHECK ====================

@app.route('/health')
def health_check():
    """Health check endpoint"""
    db_status = 'connected' if DB_AVAILABLE and check_database_connection() else 'disconnected'
    
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'database': db_status,
        'bot': 'available' if BOT_AVAILABLE else 'unavailable',
        'version': '1.0.0'
    })

# ==================== MAIN ====================

if __name__ == '__main__':
    # Database ni ishga tushirish
    if DB_AVAILABLE:
        try:
            init_db()
            print("✅ PostgreSQL database ishga tushirildi")
        except Exception as e:
            print(f"⚠️ Database ishga tushirishda xato: {e}")
    else:
        print("❌ Database ulanmagan")
    
    # Botni alohida threadda ishga tushirish
    if BOT_AVAILABLE:
        try:
            bot_thread = threading.Thread(target=start_bot, daemon=True)
            bot_thread.start()
            print("✅ Bot thread ishga tushirildi")
        except Exception as e:
            print(f"⚠️ Bot thread ishga tushirishda xato: {e}")
    else:
        print("⚠️ Bot mavjud emas")
    
    # Portni environment dan olish yoki default
    port = int(os.environ.get('PORT', 10000))
    
    # Flask serverni ishga tushirish
    print(f"🚀 Web admin panel ishga tushmoqda...")
    print(f"🌐 Port: {port}")
    print(f"📊 Admin panel tayyor")
    print(f"🤖 Bot status: {'Online' if BOT_AVAILABLE else 'Offline'}")
    print(f"🗄️ Database status: {'Online' if DB_AVAILABLE else 'Offline'}")
    
    # Debug mode'ni environment dan olish
    debug_mode = os.environ.get('DEBUG', 'False').lower() == 'true'
    
    app.run(host='0.0.0.0', port=port, debug=debug_mode, use_reloader=False)