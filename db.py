# db.py
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import logging
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor, DictCursor
import pytz
from contextlib import contextmanager
import json

# Logger sozlash
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Timezone
TZ = pytz.timezone('Asia/Tashkent')

# Database connection pool
class Database:
    _instance = None
    _connection_pool = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
            cls._instance.init_pool()
        return cls._instance
    
    def init_pool(self):
        """Database connection pool yaratish"""
        try:
            DATABASE_URL = os.getenv('DATABASE_URL')
            
            if not DATABASE_URL:
                # Agar environment variable bo'lmasa, local database
                DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/garajhub"
                logger.warning("DATABASE_URL environment variable o'rnatilmagan, default local URI ishlatilmoqda")
            
            # Parse connection string
            self._connection_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                dsn=DATABASE_URL
            )
            
            # Test connection
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                logger.info("PostgreSQL ga muvaffaqiyatli ulanildi")
        except Exception as e:
            logger.error(f"PostgreSQL ga ulanib bo'lmadi: {e}")
            self._connection_pool = None
    
    @contextmanager
    def get_connection(self):
        """Database connection olish"""
        if self._connection_pool is None:
            self.init_pool()
        
        conn = None
        try:
            conn = self._connection_pool.getconn()
            yield conn
        except Exception as e:
            logger.error(f"Connection error: {e}")
            raise
        finally:
            if conn:
                self._connection_pool.putconn(conn)
    
    @contextmanager
    def get_cursor(self, cursor_factory=None):
        """Database cursor olish"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise
            finally:
                cursor.close()

# Singleton database instance
db_instance = Database()

# =========== DATABASE INITIALIZATION ===========

def init_db():
    """Database tablelarini yaratish"""
    create_tables_sql = """
    -- Users table
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        user_id BIGINT UNIQUE NOT NULL,
        username VARCHAR(100),
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        phone VARCHAR(20),
        bio TEXT,
        joined_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        last_seen TIMESTAMP WITH TIME ZONE,
        status VARCHAR(20) DEFAULT 'active'
    );
    
    -- Indexes for users
    CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id);
    CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
    CREATE INDEX IF NOT EXISTS idx_users_joined_at ON users(joined_at);
    CREATE INDEX IF NOT EXISTS idx_users_status ON users(status);
    
    -- Startups table
    CREATE TABLE IF NOT EXISTS startups (
        id SERIAL PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        description TEXT,
        logo VARCHAR(500),
        group_link VARCHAR(500),
        owner_id BIGINT NOT NULL REFERENCES users(user_id),
        status VARCHAR(20) DEFAULT 'pending',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        started_at TIMESTAMP WITH TIME ZONE,
        ended_at TIMESTAMP WITH TIME ZONE,
        results TEXT,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Indexes for startups
    CREATE INDEX IF NOT EXISTS idx_startups_owner_id ON startups(owner_id);
    CREATE INDEX IF NOT EXISTS idx_startups_status ON startups(status);
    CREATE INDEX IF NOT EXISTS idx_startups_created_at ON startups(created_at);
    CREATE INDEX IF NOT EXISTS idx_startups_status_created ON startups(status, created_at DESC);
    
    -- Startup members table
    CREATE TABLE IF NOT EXISTS startup_members (
        id SERIAL PRIMARY KEY,
        startup_id INTEGER NOT NULL REFERENCES startups(id) ON DELETE CASCADE,
        user_id BIGINT NOT NULL REFERENCES users(user_id),
        status VARCHAR(20) DEFAULT 'pending',
        joined_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(startup_id, user_id)
    );
    
    -- Indexes for startup_members
    CREATE INDEX IF NOT EXISTS idx_startup_members_startup_id ON startup_members(startup_id);
    CREATE INDEX IF NOT EXISTS idx_startup_members_user_id ON startup_members(user_id);
    CREATE INDEX IF NOT EXISTS idx_startup_members_status ON startup_members(status);
    CREATE INDEX IF NOT EXISTS idx_startup_members_startup_status ON startup_members(startup_id, status);
    
    -- Messages table for broadcast
    CREATE TABLE IF NOT EXISTS broadcast_messages (
        id SERIAL PRIMARY KEY,
        message TEXT NOT NULL,
        recipient_type VARCHAR(20) DEFAULT 'all',
        sent_by VARCHAR(100),
        sent_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        sent_count INTEGER DEFAULT 0,
        failed_count INTEGER DEFAULT 0
    );
    
    -- Admin activity log
    CREATE TABLE IF NOT EXISTS admin_logs (
        id SERIAL PRIMARY KEY,
        admin_username VARCHAR(100),
        action VARCHAR(200),
        details JSONB,
        ip_address VARCHAR(45),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Analytics cache
    CREATE TABLE IF NOT EXISTS analytics_cache (
        id SERIAL PRIMARY KEY,
        cache_key VARCHAR(100) UNIQUE NOT NULL,
        data JSONB NOT NULL,
        expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    try:
        with db_instance.get_cursor() as cur:
            cur.execute(create_tables_sql)
        logger.info("Database tablelari muvaffaqiyatli yaratildi/yangilandi")
        return True
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        return False

# =========== HELPER FUNCTIONS ===========

def _format_timestamp(value):
    """Timestamp ni formatlash"""
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(TZ).strftime('%Y-%m-%d %H:%M')
    return value

def _parse_timestamp(value):
    """String dan timestamp ga o'tkazish"""
    if not value:
        return None
    try:
        if isinstance(value, str):
            return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(TZ)
        return value
    except:
        return value

def _dict_to_json(value):
    """Dict ni JSON string ga o'tkazish"""
    if value is None:
        return None
    if isinstance(value, dict) or isinstance(value, list):
        return json.dumps(value)
    return value

def _json_to_dict(value):
    """JSON string dan dict ga o'tkazish"""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except:
            return value
    return value

# =========== USERS FUNCTIONS ===========

def get_user(user_id: int) -> Optional[Dict]:
    """Foydalanuvchini ID bo'yicha olish"""
    try:
        with db_instance.get_cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, user_id, username, first_name, last_name, 
                       phone, bio, joined_at, updated_at, last_seen, status
                FROM users 
                WHERE user_id = %s
            """, (user_id,))
            user = cur.fetchone()
            if user:
                user['joined_at'] = _format_timestamp(user.get('joined_at'))
                user['updated_at'] = _format_timestamp(user.get('updated_at'))
                user['last_seen'] = _format_timestamp(user.get('last_seen'))
            return dict(user) if user else None
    except Exception as e:
        logger.error(f"Error getting user {user_id}: {e}")
        return None

def save_user(user_id: int, username: str, first_name: str) -> bool:
    """Yangi foydalanuvchi qo'shish yoki mavjudni yangilash"""
    try:
        with db_instance.get_cursor() as cur:
            cur.execute("""
                INSERT INTO users (user_id, username, first_name, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id) 
                DO UPDATE SET 
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    updated_at = CURRENT_TIMESTAMP
            """, (user_id, username, first_name))
        logger.info(f"Foydalanuvchi saqlandi: {user_id}")
        return True
    except Exception as e:
        logger.error(f"Error saving user {user_id}: {e}")
        return False

def update_user_field(user_id: int, field: str, value: str) -> bool:
    """Foydalanuvchi maydonini yangilash"""
    try:
        with db_instance.get_cursor() as cur:
            # Field name validation
            valid_fields = ['username', 'first_name', 'last_name', 'phone', 'bio', 'status']
            if field not in valid_fields:
                raise ValueError(f"Invalid field: {field}")
            
            cur.execute(f"""
                UPDATE users 
                SET {field} = %s, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = %s
            """, (value, user_id))
        logger.info(f"Foydalanuvchi maydoni yangilandi: {user_id}.{field}")
        return True
    except Exception as e:
        logger.error(f"Error updating user field {user_id}.{field}: {e}")
        return False

def get_user_by_username(username: str) -> Optional[Dict]:
    """Foydalanuvchini username bo'yicha olish"""
    try:
        with db_instance.get_cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM users 
                WHERE username = %s
            """, (username,))
            user = cur.fetchone()
            return dict(user) if user else None
    except Exception as e:
        logger.error(f"Error getting user by username {username}: {e}")
        return None

def get_all_users() -> List[int]:
    """Barcha foydalanuvchi ID larini olish"""
    try:
        with db_instance.get_cursor() as cur:
            cur.execute("SELECT user_id FROM users WHERE status = 'active'")
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Error getting all users: {e}")
        return []

def get_recent_users(limit: int = 10) -> List[Dict]:
    """So'nggi foydalanuvchilar"""
    try:
        with db_instance.get_cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT user_id, username, first_name, last_name, 
                       phone, joined_at, status
                FROM users 
                ORDER BY joined_at DESC 
                LIMIT %s
            """, (limit,))
            users = cur.fetchall()
            
            formatted_users = []
            for user in users:
                user_dict = dict(user)
                user_dict['joined_at'] = _format_timestamp(user_dict.get('joined_at'))
                formatted_users.append(user_dict)
            
            return formatted_users
    except Exception as e:
        logger.error(f"Error getting recent users: {e}")
        return []

# =========== STARTUPS FUNCTIONS ===========

def create_startup(name: str, description: str, logo: str, group_link: str, owner_id: int) -> Optional[int]:
    """Yangi startup yaratish"""
    try:
        with db_instance.get_cursor() as cur:
            cur.execute("""
                INSERT INTO startups 
                (name, description, logo, group_link, owner_id, status, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                RETURNING id
            """, (name, description, logo, group_link, owner_id))
            
            startup_id = cur.fetchone()[0]
            logger.info(f"Yangi startup yaratildi: {startup_id} - {name}")
            return startup_id
    except Exception as e:
        logger.error(f"Error creating startup: {e}")
        return None

def get_startup(startup_id: int) -> Optional[Dict]:
    """Startupni ID bo'yicha olish"""
    try:
        with db_instance.get_cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT s.*, 
                       u.first_name as owner_first_name,
                       u.last_name as owner_last_name,
                       u.username as owner_username,
                       u.phone as owner_phone
                FROM startups s
                LEFT JOIN users u ON s.owner_id = u.user_id
                WHERE s.id = %s
            """, (startup_id,))
            
            startup = cur.fetchone()
            if not startup:
                return None
            
            startup_dict = dict(startup)
            
            # Format timestamps
            timestamp_fields = ['created_at', 'started_at', 'ended_at', 'updated_at']
            for field in timestamp_fields:
                startup_dict[field] = _format_timestamp(startup_dict.get(field))
            
            return startup_dict
    except Exception as e:
        logger.error(f"Error getting startup {startup_id}: {e}")
        return None

def get_startups_by_owner(owner_id: int) -> List[Dict]:
    """Muallif ID bo'yicha startuplarni olish"""
    try:
        with db_instance.get_cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM startups 
                WHERE owner_id = %s 
                ORDER BY created_at DESC
            """, (owner_id,))
            
            startups = cur.fetchall()
            formatted_startups = []
            
            for startup in startups:
                startup_dict = dict(startup)
                startup_dict['created_at'] = _format_timestamp(startup_dict.get('created_at'))
                startup_dict['updated_at'] = _format_timestamp(startup_dict.get('updated_at'))
                formatted_startups.append(startup_dict)
            
            logger.debug(f"{owner_id} uchun {len(formatted_startups)} ta startup topildi")
            return formatted_startups
    except Exception as e:
        logger.error(f"Error getting startups for owner {owner_id}: {e}")
        return []

def _paginate_startups(query: str, params: tuple, page: int, per_page: int) -> Tuple[List[Dict], int]:
    """Pagination helper funksiyasi"""
    try:
        with db_instance.get_cursor(cursor_factory=RealDictCursor) as cur:
            # Total count
            count_query = f"SELECT COUNT(*) as total FROM ({query}) as subquery"
            cur.execute(count_query, params)
            total = cur.fetchone()['total']
            
            # Paginated data
            data_query = f"{query} LIMIT %s OFFSET %s"
            offset = (page - 1) * per_page
            cur.execute(data_query, params + (per_page, offset))
            
            startups = cur.fetchall()
            formatted_startups = []
            
            for startup in startups:
                startup_dict = dict(startup)
                startup_dict['created_at'] = _format_timestamp(startup_dict.get('created_at'))
                startup_dict['updated_at'] = _format_timestamp(startup_dict.get('updated_at'))
                formatted_startups.append(startup_dict)
            
            return formatted_startups, total
    except Exception as e:
        logger.error(f"Pagination error: {e}")
        return [], 0

def get_pending_startups(page: int = 1, per_page: int = 10) -> Tuple[List[Dict], int]:
    """Kutilayotgan startuplar"""
    query = """
        SELECT s.*, 
               u.first_name as owner_first_name,
               u.last_name as owner_last_name
        FROM startups s
        LEFT JOIN users u ON s.owner_id = u.user_id
        WHERE s.status = 'pending'
        ORDER BY s.created_at DESC
    """
    return _paginate_startups(query, (), page, per_page)

def get_active_startups(page: int = 1, per_page: int = 10) -> Tuple[List[Dict], int]:
    """Faol startuplar"""
    query = """
        SELECT s.*, 
               u.first_name as owner_first_name,
               u.last_name as owner_last_name
        FROM startups s
        LEFT JOIN users u ON s.owner_id = u.user_id
        WHERE s.status = 'active'
        ORDER BY s.created_at DESC
    """
    return _paginate_startups(query, (), page, per_page)

def get_completed_startups(page: int = 1, per_page: int = 10) -> Tuple[List[Dict], int]:
    """Yakunlangan startuplar"""
    query = """
        SELECT s.*, 
               u.first_name as owner_first_name,
               u.last_name as owner_last_name
        FROM startups s
        LEFT JOIN users u ON s.owner_id = u.user_id
        WHERE s.status = 'completed'
        ORDER BY s.created_at DESC
    """
    return _paginate_startups(query, (), page, per_page)

def get_rejected_startups(page: int = 1, per_page: int = 10) -> Tuple[List[Dict], int]:
    """Rad etilgan startuplar"""
    query = """
        SELECT s.*, 
               u.first_name as owner_first_name,
               u.last_name as owner_last_name
        FROM startups s
        LEFT JOIN users u ON s.owner_id = u.user_id
        WHERE s.status = 'rejected'
        ORDER BY s.created_at DESC
    """
    return _paginate_startups(query, (), page, per_page)

def update_startup_status(startup_id: int, status: str) -> bool:
    """Startup holatini yangilash"""
    try:
        update_fields = {
            'status': status,
            'updated_at': 'CURRENT_TIMESTAMP'
        }
        
        if status == 'active':
            update_fields['started_at'] = 'CURRENT_TIMESTAMP'
        elif status == 'completed':
            update_fields['ended_at'] = 'CURRENT_TIMESTAMP'
        
        set_clause = ', '.join([f"{k} = {v}" for k, v in update_fields.items()])
        
        with db_instance.get_cursor() as cur:
            cur.execute(f"""
                UPDATE startups 
                SET {set_clause}
                WHERE id = %s
            """, (startup_id,))
            
            if cur.rowcount > 0:
                logger.info(f"Startup holati yangilandi: {startup_id} -> {status}")
                return True
            else:
                logger.warning(f"Startup holati yangilanmadi: {startup_id}")
                return False
    except Exception as e:
        logger.error(f"Error updating startup status {startup_id}: {e}")
        return False

def update_startup_results(startup_id: int, results: str) -> bool:
    """Startup natijalarini yangilash"""
    try:
        with db_instance.get_cursor() as cur:
            cur.execute("""
                UPDATE startups 
                SET results = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (results, startup_id))
            
            if cur.rowcount > 0:
                logger.info(f"Startup natijalari yangilandi: {startup_id}")
                return True
            else:
                logger.warning(f"Startup natijalari yangilanmadi: {startup_id}")
                return False
    except Exception as e:
        logger.error(f"Error updating startup results {startup_id}: {e}")
        return False

def search_startups(search_query: str, page: int = 1, per_page: int = 10) -> Tuple[List[Dict], int]:
    """Startaplarni qidirish"""
    query = """
        SELECT s.*, 
               u.first_name as owner_first_name,
               u.last_name as owner_last_name
        FROM startups s
        LEFT JOIN users u ON s.owner_id = u.user_id
        WHERE s.name ILIKE %s OR s.description ILIKE %s
        ORDER BY s.created_at DESC
    """
    search_pattern = f"%{search_query}%"
    return _paginate_startups(query, (search_pattern, search_pattern), page, per_page)

# =========== STARTUP MEMBERS FUNCTIONS ===========

def add_startup_member(startup_id: int, user_id: int) -> Optional[int]:
    """Startupga a'zo qo'shish"""
    try:
        with db_instance.get_cursor() as cur:
            cur.execute("""
                INSERT INTO startup_members 
                (startup_id, user_id, status, joined_at, updated_at)
                VALUES (%s, %s, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (startup_id, user_id) 
                DO UPDATE SET updated_at = CURRENT_TIMESTAMP
                RETURNING id
            """, (startup_id, user_id))
            
            result = cur.fetchone()
            if result:
                member_id = result[0]
                logger.info(f"Yangi startup a'zosi qo'shildi: {member_id}")
                return member_id
            return None
    except Exception as e:
        logger.error(f"Error adding startup member: {e}")
        return None

def get_join_request_id(startup_id: int, user_id: int) -> Optional[int]:
    """Qo'shilish so'rovi ID sini olish"""
    try:
        with db_instance.get_cursor() as cur:
            cur.execute("""
                SELECT id FROM startup_members 
                WHERE startup_id = %s AND user_id = %s
            """, (startup_id, user_id))
            
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as e:
        logger.error(f"Error getting join request: {e}")
        return None

def update_join_request(request_id: int, status: str) -> bool:
    """Qo'shilish so'rovini yangilash"""
    try:
        with db_instance.get_cursor() as cur:
            cur.execute("""
                UPDATE startup_members 
                SET status = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (status, request_id))
            
            if cur.rowcount > 0:
                logger.info(f"Join request yangilandi: {request_id} -> {status}")
                return True
            else:
                logger.warning(f"Join request yangilanmadi: {request_id}")
                return False
    except Exception as e:
        logger.error(f"Error updating join request {request_id}: {e}")
        return False

def get_startup_members(startup_id: int, page: int = 1, per_page: int = 10) -> Tuple[List[Dict], int]:
    """Startup a'zolarini olish"""
    try:
        # Total count
        with db_instance.get_cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) as total
                FROM startup_members sm
                JOIN users u ON sm.user_id = u.user_id
                WHERE sm.startup_id = %s AND sm.status = 'accepted'
            """, (startup_id,))
            total = cur.fetchone()[0]
        
        # Members data
        with db_instance.get_cursor(cursor_factory=RealDictCursor) as cur:
            offset = (page - 1) * per_page
            cur.execute("""
                SELECT u.user_id, u.first_name, u.last_name, 
                       u.username, u.phone, u.bio, sm.joined_at
                FROM startup_members sm
                JOIN users u ON sm.user_id = u.user_id
                WHERE sm.startup_id = %s AND sm.status = 'accepted'
                ORDER BY sm.joined_at DESC
                LIMIT %s OFFSET %s
            """, (startup_id, per_page, offset))
            
            members = cur.fetchall()
            formatted_members = []
            
            for member in members:
                member_dict = dict(member)
                member_dict['joined_at'] = _format_timestamp(member_dict.get('joined_at'))
                formatted_members.append(member_dict)
            
            return formatted_members, total
    except Exception as e:
        logger.error(f"Error getting startup members {startup_id}: {e}")
        return [], 0

def get_user_startups(user_id: int) -> List[Dict]:
    """Foydalanuvchi a'zo bo'lgan startaplar"""
    try:
        with db_instance.get_cursor(cursor_factory=RealDictCursor) as cur:
            # As owner
            cur.execute("""
                SELECT s.* FROM startups s
                WHERE s.owner_id = %s
                ORDER BY s.created_at DESC
            """, (user_id,))
            owned_startups = cur.fetchall()
            
            # As member
            cur.execute("""
                SELECT s.* FROM startups s
                JOIN startup_members sm ON s.id = sm.startup_id
                WHERE sm.user_id = %s AND sm.status = 'accepted'
                ORDER BY s.created_at DESC
            """, (user_id,))
            member_startups = cur.fetchall()
            
            all_startups = owned_startups + member_startups
            formatted_startups = []
            
            for startup in all_startups:
                startup_dict = dict(startup)
                startup_dict['created_at'] = _format_timestamp(startup_dict.get('created_at'))
                startup_dict['updated_at'] = _format_timestamp(startup_dict.get('updated_at'))
                formatted_startups.append(startup_dict)
            
            return formatted_startups
    except Exception as e:
        logger.error(f"Error getting user startups {user_id}: {e}")
        return []

def get_all_startup_members(startup_id: int) -> List[int]:
    """Startupning barcha a'zolari (faqat user_id lar)"""
    try:
        with db_instance.get_cursor() as cur:
            cur.execute("""
                SELECT user_id FROM startup_members 
                WHERE startup_id = %s AND status = 'accepted'
            """, (startup_id,))
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Error getting all startup members {startup_id}: {e}")
        return []

# =========== STATISTICS FUNCTIONS ===========

def get_statistics() -> Dict:
    """Umumiy statistika"""
    try:
        with db_instance.get_cursor(cursor_factory=RealDictCursor) as cur:
            # Total users
            cur.execute("SELECT COUNT(*) as total_users FROM users")
            total_users = cur.fetchone()['total_users']
            
            # Total startups
            cur.execute("SELECT COUNT(*) as total_startups FROM startups")
            total_startups = cur.fetchone()['total_startups']
            
            # Startups by status
            cur.execute("""
                SELECT 
                    COUNT(CASE WHEN status = 'active' THEN 1 END) as active_startups,
                    COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_startups,
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_startups,
                    COUNT(CASE WHEN status = 'rejected' THEN 1 END) as rejected_startups
                FROM startups
            """)
            status_counts = cur.fetchone()
            
            # Today's new users
            cur.execute("""
                SELECT COUNT(*) as new_users_today 
                FROM users 
                WHERE DATE(joined_at) = CURRENT_DATE
            """)
            new_users_today = cur.fetchone()['new_users_today']
            
            # Last week's new users
            cur.execute("""
                SELECT COUNT(*) as new_users_last_week 
                FROM users 
                WHERE joined_at >= CURRENT_DATE - INTERVAL '7 days'
            """)
            new_users_last_week = cur.fetchone()['new_users_last_week']
            
            # Average daily users
            avg_daily_users = round(new_users_last_week / 7, 1) if new_users_last_week > 0 else 0
            
            return {
                'total_users': total_users,
                'total_startups': total_startups,
                'active_startups': status_counts['active_startups'],
                'pending_startups': status_counts['pending_startups'],
                'completed_startups': status_counts['completed_startups'],
                'rejected_startups': status_counts['rejected_startups'],
                'new_users_today': new_users_today,
                'new_users_last_week': new_users_last_week,
                'avg_daily_users': avg_daily_users
            }
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        return {}

def get_user_activity_stats(user_id: int) -> Dict:
    """Foydalanuvchi faollik statistikasi"""
    try:
        with db_instance.get_cursor(cursor_factory=RealDictCursor) as cur:
            # Owned startups
            cur.execute("""
                SELECT 
                    COUNT(*) as owned_startups,
                    COUNT(CASE WHEN status = 'active' THEN 1 END) as active_owned,
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_owned
                FROM startups 
                WHERE owner_id = %s
            """, (user_id,))
            owned_stats = cur.fetchone()
            
            # Joined startups
            cur.execute("""
                SELECT COUNT(*) as joined_startups 
                FROM startup_members 
                WHERE user_id = %s AND status = 'accepted'
            """, (user_id,))
            joined_startups = cur.fetchone()['joined_startups']
            
            return {
                'owned_startups': owned_stats['owned_startups'],
                'joined_startups': joined_startups,
                'active_owned': owned_stats['active_owned'],
                'completed_owned': owned_stats['completed_owned'],
                'total_participation': owned_stats['owned_startups'] + joined_startups
            }
    except Exception as e:
        logger.error(f"Error getting user activity stats {user_id}: {e}")
        return {}

def get_recent_startups(limit: int = 10) -> List[Dict]:
    """So'nggi startuplar"""
    try:
        with db_instance.get_cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT s.*, 
                       u.first_name as owner_first_name,
                       u.last_name as owner_last_name
                FROM startups s
                LEFT JOIN users u ON s.owner_id = u.user_id
                ORDER BY s.created_at DESC 
                LIMIT %s
            """, (limit,))
            
            startups = cur.fetchall()
            formatted_startups = []
            
            for startup in startups:
                startup_dict = dict(startup)
                startup_dict['created_at'] = _format_timestamp(startup_dict.get('created_at'))
                formatted_startups.append(startup_dict)
            
            return formatted_startups
    except Exception as e:
        logger.error(f"Error getting recent startups: {e}")
        return []

# =========== UTILITY FUNCTIONS ===========

def check_database_connection() -> bool:
    """Database ulanishini tekshirish"""
    try:
        with db_instance.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        logger.info("Database ga muvaffaqiyatli ulanildi")
        return True
    except Exception as e:
        logger.error(f"Database ga ulanib bo'lmadi: {e}")
        return False

def get_collection_stats() -> Dict:
    """Database statistikasi"""
    try:
        with db_instance.get_cursor(cursor_factory=RealDictCursor) as cur:
            tables = ['users', 'startups', 'startup_members', 'broadcast_messages', 'admin_logs']
            stats = {}
            
            for table in tables:
                cur.execute(f"""
                    SELECT 
                        COUNT(*) as count,
                        pg_size_pretty(pg_total_relation_size(%s)) as size
                    FROM {table}
                """, (table,))
                
                table_stats = cur.fetchone()
                if table_stats:
                    stats[table] = dict(table_stats)
            
            return stats
    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")
        return {}

def save_broadcast_message(message: str, sent_by: str, sent_count: int, failed_count: int, recipient_type: str = 'all') -> int:
    """Broadcast xabarini saqlash"""
    try:
        with db_instance.get_cursor() as cur:
            cur.execute("""
                INSERT INTO broadcast_messages 
                (message, sent_by, sent_count, failed_count, recipient_type, sent_at)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                RETURNING id
            """, (message, sent_by, sent_count, failed_count, recipient_type))
            
            return cur.fetchone()[0]
    except Exception as e:
        logger.error(f"Error saving broadcast message: {e}")
        return 0

def log_admin_action(admin_username: str, action: str, details: Dict = None, ip_address: str = None):
    """Admin harakatlarini log qilish"""
    try:
        with db_instance.get_cursor() as cur:
            cur.execute("""
                INSERT INTO admin_logs 
                (admin_username, action, details, ip_address, created_at)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            """, (admin_username, action, _dict_to_json(details), ip_address))
    except Exception as e:
        logger.error(f"Error logging admin action: {e}")

# Import timedelta
from datetime import timedelta

# Database ulanishini tekshirish
if __name__ == '__main__':
    connection_status = check_database_connection()
    if connection_status:
        print("✅ PostgreSQL ga muvaffaqiyatli ulanildi")
        init_db()
        stats = get_statistics()
        print(f"📊 Database statistikasi:")
        print(f"   👥 Foydalanuvchilar: {stats.get('total_users', 0)}")
        print(f"   🚀 Startaplar: {stats.get('total_startups', 0)}")
        print(f"   ▶️ Faol startaplar: {stats.get('active_startups', 0)}")
        print(f"   ⏳ Kutilayotgan: {stats.get('pending_startups', 0)}")
    else:
        print("❌ PostgreSQL ga ulanib bo'lmadi")