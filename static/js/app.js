// Asosiy JavaScript fayli

// Global o'zgaruvchilar
let currentUser = null;
let charts = {};
let currentPage = 'dashboard';

// DOM yuklanganda
document.addEventListener('DOMContentLoaded', function() {
    // Auth tekshirish
    checkAuth();
    
    // Event listener'lar
    setupEventListeners();
    
    // Dashboard statistikasini yuklash
    loadDashboard();
    
    // Loading ekranini yashirish
    setTimeout(() => {
        document.getElementById('loadingScreen').style.display = 'none';
    }, 1000);
});

// Auth tekshirish
async function checkAuth() {
    try {
        const response = await fetch('/api/check_auth');
        const data = await response.json();
        
        if (data.authenticated) {
            currentUser = data.user;
            showPage('dashboard');
            updateUserInfo();
        } else {
            showPage('login');
        }
    } catch (error) {
        console.error('Auth tekshirish xatosi:', error);
        showPage('login');
    }
}

// Event listener'lar
function setupEventListeners() {
    // Login form
    document.getElementById('loginForm')?.addEventListener('submit', handleLogin);
    
    // Logout
    document.getElementById('logoutBtn')?.addEventListener('click', handleLogout);
    
    // Menu items
    document.querySelectorAll('.menu li').forEach(item => {
        item.addEventListener('click', function() {
            const page = this.dataset.page;
            if (page) {
                showPage(page);
                setActiveMenuItem(page);
            }
        });
    });
    
    // Theme toggle
    const themeToggle = document.getElementById('themeToggle');
    if (themeToggle) {
        themeToggle.addEventListener('change', toggleTheme);
        
        // Saqlangan temani yuklash
        const savedTheme = localStorage.getItem('theme') || 'light';
        document.documentElement.setAttribute('data-theme', savedTheme);
        themeToggle.checked = savedTheme === 'dark';
    }
    
    // Password ko'rish
    document.getElementById('togglePassword')?.addEventListener('click', function() {
        const passwordInput = document.getElementById('loginPassword');
        const icon = this.querySelector('i');
        if (passwordInput.type === 'password') {
            passwordInput.type = 'text';
            icon.classList.remove('fa-eye');
            icon.classList.add('fa-eye-slash');
        } else {
            passwordInput.type = 'password';
            icon.classList.remove('fa-eye-slash');
            icon.classList.add('fa-eye');
        }
    });
    
    // Menu toggle (mobil uchun)
    document.getElementById('menuToggle')?.addEventListener('click', function() {
        document.querySelector('.sidebar').classList.toggle('active');
    });
}

// Login
async function handleLogin(e) {
    e.preventDefault();
    
    const username = document.getElementById('loginUsername').value;
    const password = document.getElementById('loginPassword').value;
    
    try {
        const response = await fetch('/api/login', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ username, password })
        });
        
        const data = await response.json();
        
        if (data.success) {
            currentUser = data.user;
            showPage('dashboard');
            updateUserInfo();
            showToast('Muvaffaqiyatli kirildi', 'success');
        } else {
            showToast(data.error || 'Login xatosi', 'error');
        }
    } catch (error) {
        console.error('Login xatosi:', error);
        showToast('Server xatosi', 'error');
    }
}

// Logout
async function handleLogout() {
    try {
        const response = await fetch('/api/logout', { method: 'POST' });
        const data = await response.json();
        
        if (data.success) {
            currentUser = null;
            showPage('login');
            showToast('Muvaffaqiyatli chiqildi', 'success');
        }
    } catch (error) {
        console.error('Logout xatosi:', error);
    }
}

// Sahifani ko'rsatish
function showPage(pageId) {
    currentPage = pageId;
    
    // Barcha sahifalarni yashirish
    document.querySelectorAll('.page').forEach(page => {
        page.classList.remove('active');
    });
    
    // Kerakli sahifani ko'rsatish
    const pageElement = document.getElementById(pageId + 'Page');
    if (pageElement) {
        pageElement.classList.add('active');
        
        // Agar dashboard bo'lsa, statistikani yuklash
        if (pageId === 'dashboard') {
            loadDashboard();
        }
        // Agar foydalanuvchilar sahifasi bo'lsa
        else if (pageId === 'users') {
            loadUsers();
        }
        // Agar startaplar sahifasi bo'lsa
        else if (pageId === 'startups') {
            loadStartups();
        }
        // Agar statistika sahifasi bo'lsa
        else if (pageId === 'statistics') {
            loadDetailedStatistics();
        }
        // Agar adminlar sahifasi bo'lsa
        else if (pageId === 'admins') {
            loadAdmins();
        }
        // Agar backup sahifasi bo'lsa
        else if (pageId === 'backup') {
            loadBackups();
        }
        // Agar sozlamalar sahifasi bo'lsa
        else if (pageId === 'settings') {
            loadSettings();
        }
    }
}

// Menu itemni faol qilish
function setActiveMenuItem(pageId) {
    document.querySelectorAll('.menu li').forEach(item => {
        item.classList.remove('active');
    });
    
    const activeItem = document.querySelector(`.menu li[data-page="${pageId}"]`);
    if (activeItem) {
        activeItem.classList.add('active');
    }
}

// Dashboard yuklash
async function loadDashboard() {
    try {
        const response = await fetch('/api/statistics');
        const data = await response.json();
        
        if (data.success) {
            updateStatistics(data.data);
            loadUserGrowthChart();
            loadStartupDistributionChart();
            loadRecentActivity();
        }
    } catch (error) {
        console.error('Dashboard yuklash xatosi:', error);
        showToast('Statistikani yuklashda xatolik', 'error');
    }
}

// Statistika yangilash
function updateStatistics(stats) {
    // Asosiy statistika
    document.getElementById('totalUsers').textContent = stats.total_users.toLocaleString();
    document.getElementById('totalStartups').textContent = stats.total_startups.toLocaleString();
    document.getElementById('activeStartups').textContent = stats.active_startups.toLocaleString();
    document.getElementById('newToday').textContent = stats.new_today.toLocaleString();
    document.getElementById('activityRate').textContent = stats.activity_rate + '%';
    
    // Progress bar
    document.getElementById('activityProgress').style.width = stats.activity_rate + '%';
    
    // Trendlar
    if (stats.trends) {
        document.getElementById('startupTrend').innerHTML = `
            <i class="fas fa-arrow-up"></i>
            <span>${stats.trends.startups}</span>
        `;
        document.getElementById('activeStartupCount').textContent = `${stats.active_startups} ta`;
        document.getElementById('newUsersToday').textContent = `${stats.new_today} user`;
    }
}

// Foydalanuvchi o'sishi grafigi
async function loadUserGrowthChart() {
    try {
        const period = document.getElementById('growthFilter')?.value || 'month';
        const response = await fetch(`/api/analytics/user-growth?period=${period}`);
        const data = await response.json();
        
        if (data.success) {
            const ctx = document.getElementById('userGrowthChart')?.getContext('2d');
            if (!ctx) return;
            
            // Avvalgi chartni yo'q qilish
            if (charts.userGrowth) {
                charts.userGrowth.destroy();
            }
            
            // Yangi chart yaratish
            charts.userGrowth = new Chart(ctx, {
                type: 'line',
                data: data.data,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            display: true,
                            position: 'top',
                            labels: {
                                color: getComputedStyle(document.documentElement).getPropertyValue('--text-primary')
                            }
                        }
                    },
                    scales: {
                        x: {
                            grid: {
                                color: getComputedStyle(document.documentElement).getPropertyValue('--border-color')
                            },
                            ticks: {
                                color: getComputedStyle(document.documentElement).getPropertyValue('--text-secondary')
                            }
                        },
                        y: {
                            grid: {
                                color: getComputedStyle(document.documentElement).getPropertyValue('--border-color')
                            },
                            ticks: {
                                color: getComputedStyle(document.documentElement).getPropertyValue('--text-secondary')
                            }
                        }
                    }
                }
            });
        }
    } catch (error) {
        console.error('Chart yuklash xatosi:', error);
    }
}

// Startap taqsimoti grafigi
async function loadStartupDistributionChart() {
    try {
        const response = await fetch('/api/analytics/startup-distribution');
        const data = await response.json();
        
        if (data.success) {
            const ctx = document.getElementById('startupDistributionChart')?.getContext('2d');
            if (!ctx) return;
            
            // Avvalgi chartni yo'q qilish
            if (charts.startupDistribution) {
                charts.startupDistribution.destroy();
            }
            
            // Jami startaplar soni
            document.getElementById('totalStartupsChart').textContent = data.total;
            
            // Legend yaratish
            const legendContainer = document.getElementById('distributionLegend');
            if (legendContainer) {
                let legendHTML = '';
                data.data.labels.forEach((label, index) => {
                    const value = data.data.datasets[0].data[index];
                    const color = data.data.datasets[0].backgroundColor[index];
                    legendHTML += `
                        <div class="legend-item">
                            <div class="legend-color" style="background-color: ${color}"></div>
                            <div class="legend-text">${label}</div>
                            <div class="legend-value">${value}</div>
                        </div>
                    `;
                });
                legendContainer.innerHTML = legendHTML;
            }
            
            // Yangi chart yaratish
            charts.startupDistribution = new Chart(ctx, {
                type: 'doughnut',
                data: data.data,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    cutout: '70%',
                    plugins: {
                        legend: {
                            display: false
                        }
                    }
                }
            });
        }
    } catch (error) {
        console.error('Distribution chart xatosi:', error);
    }
}

// So'nggi faollik
async function loadRecentActivity() {
    try {
        const response = await fetch('/api/activity');
        const data = await response.json();
        
        if (data.success) {
            const activityList = document.getElementById('activityList');
            if (!activityList) return;
            
            let activityHTML = '';
            data.data.forEach(activity => {
                const iconClass = {
                    'user': 'fa-user',
                    'startup': 'fa-rocket',
                    'message': 'fa-envelope',
                    'system': 'fa-cog'
                }[activity.icon] || 'fa-circle';
                
                activityHTML += `
                    <div class="activity-item slide-in">
                        <div class="activity-icon">
                            <i class="fas ${iconClass}"></i>
                        </div>
                        <div class="activity-content">
                            <div class="activity-text">${activity.description}</div>
                            <div class="activity-time">${activity.time_ago}</div>
                        </div>
                    </div>
                `;
            });
            
            activityList.innerHTML = activityHTML || `
                <div class="empty-state">
                    <i class="fas fa-history"></i>
                    <p>Faollik yo'q</p>
                </div>
            `;
        }
    } catch (error) {
        console.error('Activity yuklash xatosi:', error);
    }
}

// Foydalanuvchilar ro'yxati
async function loadUsers(page = 1) {
    try {
        const search = document.getElementById('userSearch')?.value || '';
        const filter = document.getElementById('userFilter')?.value || 'all';
        
        const response = await fetch(`/api/users?page=${page}&search=${search}&filter=${filter}`);
        const data = await response.json();
        
        if (data.success) {
            updateUsersTable(data.data);
            updatePagination('usersPagination', data.pagination, loadUsers);
        }
    } catch (error) {
        console.error('Foydalanuvchilar yuklash xatosi:', error);
        showToast('Foydalanuvchilarni yuklashda xatolik', 'error');
    }
}

// Foydalanuvchilar jadvalini yangilash
function updateUsersTable(users) {
    const tbody = document.getElementById('usersTableBody');
    if (!tbody) return;
    
    if (users.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="7" style="text-align: center; padding: 40px;">
                    <i class="fas fa-users" style="font-size: 32px; color: var(--text-muted); margin-bottom: 10px; display: block;"></i>
                    <p style="color: var(--text-muted);">Foydalanuvchilar topilmadi</p>
                </td>
            </tr>
        `;
        return;
    }
    
    let html = '';
    users.forEach(user => {
        html += `
            <tr>
                <td>${user.id}</td>
                <td>${user.first_name}</td>
                <td>${user.last_name}</td>
                <td>${user.phone}</td>
                <td>${formatDate(user.joined_at)}</td>
                <td><span class="status-badge ${user.status}">${user.status === 'active' ? 'Faol' : 'Faol emas'}</span></td>
                <td>
                    <div class="table-actions">
                        <button class="action-btn view-btn" onclick="viewUser('${user.id}')" title="Ko'rish">
                            <i class="fas fa-eye"></i>
                        </button>
                        <button class="action-btn edit-btn" onclick="editUser('${user.id}')" title="Tahrirlash">
                            <i class="fas fa-edit"></i>
                        </button>
                        <button class="action-btn delete-btn" onclick="deleteUser('${user.id}')" title="O'chirish">
                            <i class="fas fa-trash"></i>
                        </button>
                    </div>
                </td>
            </tr>
        `;
    });
    
    tbody.innerHTML = html;
}

// Startaplar ro'yxati
async function loadStartups(page = 1) {
    try {
        const search = document.getElementById('startupSearch')?.value || '';
        const status = document.getElementById('startupFilter')?.value || 'all';
        
        const response = await fetch(`/api/startups?page=${page}&search=${search}&status=${status}`);
        const data = await response.json();
        
        if (data.success) {
            updateStartupsTable(data.data);
            updatePagination('startupsPagination', data.pagination, loadStartups);
        }
    } catch (error) {
        console.error('Startaplar yuklash xatosi:', error);
        showToast('Startaplarni yuklashda xatolik', 'error');
    }
}

// Startaplar jadvalini yangilash
function updateStartupsTable(startups) {
    const tbody = document.getElementById('startupsTableBody');
    if (!tbody) return;
    
    if (startups.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="7" style="text-align: center; padding: 40px;">
                    <i class="fas fa-rocket" style="font-size: 32px; color: var(--text-muted); margin-bottom: 10px; display: block;"></i>
                    <p style="color: var(--text-muted);">Startaplar topilmadi</p>
                </td>
            </tr>
        `;
        return;
    }
    
    let html = '';
    startups.forEach(startup => {
        const statusClass = {
            'active': 'active',
            'pending': 'pending',
            'completed': 'completed',
            'rejected': 'rejected'
        }[startup.status] || 'pending';
        
        html += `
            <tr>
                <td>${startup.id.substring(0, 8)}...</td>
                <td><strong>${startup.name}</strong></td>
                <td>${startup.owner_name}</td>
                <td><span class="status-badge ${statusClass}">${startup.status_text}</span></td>
                <td>${formatDate(startup.created_at)}</td>
                <td>${startup.member_count}</td>
                <td>
                    <div class="table-actions">
                        <button class="action-btn view-btn" onclick="viewStartup('${startup.id}')" title="Ko'rish">
                            <i class="fas fa-eye"></i>
                        </button>
                        ${startup.status === 'pending' ? `
                            <button class="action-btn edit-btn" onclick="approveStartup('${startup.id}')" title="Tasdiqlash">
                                <i class="fas fa-check"></i>
                            </button>
                            <button class="action-btn delete-btn" onclick="rejectStartup('${startup.id}')" title="Rad etish">
                                <i class="fas fa-times"></i>
                            </button>
                        ` : ''}
                    </div>
                </td>
            </tr>
        `;
    });
    
    tbody.innerHTML = html;
}

// Startapni ko'rish
async function viewStartup(startupId) {
    try {
        const response = await fetch(`/api/startup/${startupId}`);
        const data = await response.json();
        
        if (data.success) {
            const startup = data.data;
            const modalContent = document.getElementById('startupDetailContent');
            
            if (modalContent) {
                modalContent.innerHTML = `
                    <div class="startup-details">
                        <h4>${startup.name}</h4>
                        <p><strong>Holati:</strong> ${startup.status_text}</p>
                        <p><strong>Yaratilgan sana:</strong> ${formatDate(startup.created_at)}</p>
                        <p><strong>Boshlangan sana:</strong> ${formatDate(startup.started_at)}</p>
                        <p><strong>Yakunlangan sana:</strong> ${formatDate(startup.ended_at)}</p>
                        <p><strong>Guruh havolasi:</strong> <a href="${startup.group_link}" target="_blank">${startup.group_link}</a></p>
                        
                        <div class="description-section">
                            <h5>Tavsif:</h5>
                            <p>${startup.description}</p>
                        </div>
                        
                        ${startup.results ? `
                            <div class="results-section">
                                <h5>Natijalar:</h5>
                                <p>${startup.results}</p>
                            </div>
                        ` : ''}
                        
                        ${startup.owner ? `
                            <div class="owner-section">
                                <h5>Muallif:</h5>
                                <p><strong>Ism:</strong> ${startup.owner.first_name} ${startup.owner.last_name}</p>
                                <p><strong>Telefon:</strong> ${startup.owner.phone}</p>
                                <p><strong>Bio:</strong> ${startup.owner.bio || 'Mavjud emas'}</p>
                            </div>
                        ` : ''}
                    </div>
                `;
                
                openModal('startupDetailModal');
            }
        }
    } catch (error) {
        console.error('Startap ma\'lumotlari xatosi:', error);
        showToast('Startap ma\'lumotlarini yuklashda xatolik', 'error');
    }
}

// Startapni tasdiqlash
async function approveStartup(startupId) {
    if (!confirm('Startapni tasdiqlaysizmi?')) return;
    
    try {
        const response = await fetch(`/api/startup/${startupId}/approve`, {
            method: 'POST'
        });
        const data = await response.json();
        
        if (data.success) {
            showToast('Startap tasdiqlandi', 'success');
            loadStartups(); // Ro'yxatni yangilash
        } else {
            showToast(data.error || 'Tasdiqlash xatosi', 'error');
        }
    } catch (error) {
        console.error('Tasdiqlash xatosi:', error);
        showToast('Server xatosi', 'error');
    }
}

// Startapni rad etish
async function rejectStartup(startupId) {
    if (!confirm('Startapni rad etasizmi?')) return;
    
    try {
        const response = await fetch(`/api/startup/${startupId}/reject`, {
            method: 'POST'
        });
        const data = await response.json();
        
        if (data.success) {
            showToast('Startap rad etildi', 'success');
            loadStartups(); // Ro'yxatni yangilash
        } else {
            showToast(data.error || 'Rad etish xatosi', 'error');
        }
    } catch (error) {
        console.error('Rad etish xatosi:', error);
        showToast('Server xatosi', 'error');
    }
}

// Batafsil statistika
async function loadDetailedStatistics() {
    try {
        const period = document.getElementById('statsPeriod')?.value || 'month';
        // Bu yerda batafsil statistika API chaqiriladi
        // Hozir demo ma'lumotlar ko'rsatamiz
        showToast('Statistika yuklanmoqda...', 'info');
    } catch (error) {
        console.error('Statistika yuklash xatosi:', error);
    }
}

// Adminlar ro'yxati
async function loadAdmins() {
    try {
        const response = await fetch('/api/admins');
        const data = await response.json();
        
        if (data.success) {
            const tbody = document.getElementById('adminsTableBody');
            if (tbody) {
                let html = '';
                data.data.forEach(admin => {
                    html += `
                        <tr>
                            <td>${admin.id}</td>
                            <td><strong>${admin.username}</strong></td>
                            <td>${admin.full_name}</td>
                            <td>${admin.email}</td>
                            <td><span class="status-badge ${admin.role}">${admin.role}</span></td>
                            <td>${formatDate(admin.last_login)}</td>
                            <td>
                                <div class="table-actions">
                                    ${admin.username !== currentUser?.username ? `
                                        <button class="action-btn delete-btn" onclick="deleteAdmin(${admin.id})" title="O'chirish">
                                            <i class="fas fa-trash"></i>
                                        </button>
                                    ` : ''}
                                </div>
                            </td>
                        </tr>
                    `;
                });
                tbody.innerHTML = html;
            }
        }
    } catch (error) {
        console.error('Adminlar yuklash xatosi:', error);
    }
}

// Backup yuklash
async function loadBackups() {
    try {
        const response = await fetch('/api/backups');
        const data = await response.json();
        
        if (data.success) {
            const container = document.getElementById('backupHistory');
            if (container) {
                let html = '<h3>Backup tarixi</h3>';
                data.data.forEach(backup => {
                    html += `
                        <div class="backup-item">
                            <div class="backup-info">
                                <strong>${backup.filename}</strong>
                                <span>${backup.size} • ${formatDate(backup.created_at)}</span>
                            </div>
                            <div class="backup-actions">
                                <button class="action-btn" onclick="downloadBackup('${backup.filename}')" title="Yuklab olish">
                                    <i class="fas fa-download"></i>
                                </button>
                                <button class="action-btn delete-btn" onclick="deleteBackup(${backup.id})" title="O'chirish">
                                    <i class="fas fa-trash"></i>
                                </button>
                            </div>
                        </div>
                    `;
                });
                container.innerHTML = html;
            }
        }
    } catch (error) {
        console.error('Backup yuklash xatosi:', error);
    }
}

// Sozlamalar yuklash
async function loadSettings() {
    try {
        const response = await fetch('/api/settings');
        const data = await response.json();
        
        if (data.success) {
            // Sozlamalarni formaga to'ldirish
            const settings = data.data;
            document.getElementById('siteName').value = settings.site_name;
            document.getElementById('adminEmail').value = settings.admin_email;
            document.getElementById('timezone').value = settings.timezone;
            document.getElementById('botToken').textContent = settings.bot_token;
            document.getElementById('botStatus').textContent = settings.bot_status;
        }
    } catch (error) {
        console.error('Sozlamalar yuklash xatosi:', error);
    }
}

// Foydalanuvchi ma'lumotlarini yangilash
function updateUserInfo() {
    if (currentUser) {
        const adminName = document.getElementById('adminName');
        const adminEmail = document.getElementById('adminEmail');
        const adminAvatar = document.getElementById('adminAvatar');
        
        if (adminName) adminName.textContent = currentUser.full_name;
        if (adminEmail) adminEmail.textContent = currentUser.username + '@garajhub.uz';
        if (adminAvatar) {
            adminAvatar.src = `https://ui-avatars.com/api/?name=${encodeURIComponent(currentUser.full_name)}&background=000000&color=fff`;
        }
    }
}

// Pagination yangilash
function updatePagination(elementId, pagination, callback) {
    const container = document.getElementById(elementId);
    if (!container) return;
    
    const { page, total_pages } = pagination;
    
    let html = `
        <button class="pagination-btn" ${page === 1 ? 'disabled' : ''} onclick="${callback.name}(${page - 1})">
            <i class="fas fa-chevron-left"></i>
        </button>
        
        <div class="page-numbers">
    `;
    
    // Page numbers
    const maxVisible = 5;
    let startPage = Math.max(1, page - Math.floor(maxVisible / 2));
    let endPage = Math.min(total_pages, startPage + maxVisible - 1);
    
    if (endPage - startPage + 1 < maxVisible) {
        startPage = Math.max(1, endPage - maxVisible + 1);
    }
    
    for (let i = startPage; i <= endPage; i++) {
        html += `
            <button class="pagination-btn ${i === page ? 'active' : ''}" onclick="${callback.name}(${i})">
                ${i}
            </button>
        `;
    }
    
    html += `
        </div>
        
        <button class="pagination-btn" ${page === total_pages ? 'disabled' : ''} onclick="${callback.name}(${page + 1})">
            <i class="fas fa-chevron-right"></i>
        </button>
    `;
    
    container.innerHTML = html;
}

// Modal oynalari
function openModal(modalId) {
    const modalOverlay = document.getElementById('modalOverlay');
    const modal = document.getElementById(modalId);
    
    if (modalOverlay && modal) {
        modalOverlay.classList.add('active');
        modal.style.display = 'block';
    }
}

function closeModal() {
    const modalOverlay = document.getElementById('modalOverlay');
    
    if (modalOverlay) {
        modalOverlay.classList.remove('active');
        modalOverlay.querySelectorAll('.modal').forEach(modal => {
            modal.style.display = 'none';
        });
    }
}

// Toast notification
function showToast(message, type = 'info') {
    const colors = {
        success: '#000000',
        error: '#ff4444',
        warning: '#ff8800',
        info: '#0099cc'
    };
    
    Toastify({
        text: message,
        duration: 3000,
        gravity: "top",
        position: "right",
        backgroundColor: colors[type] || colors.info,
        stopOnFocus: true,
    }).showToast();
}

// Theme toggle
function toggleTheme() {
    const theme = this.checked ? 'dark' : 'light';
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem('theme', theme);
}

// Utility funksiyalar
function formatDate(dateString) {
    if (!dateString) return 'Noma\'lum';
    
    const date = new Date(dateString);
    return date.toLocaleDateString('uz-UZ', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
    });
}

function formatNumber(num) {
    return new Intl.NumberFormat().format(num);
}

// Foydalanuvchi qidirish
function searchUsers() {
    const searchInput = document.getElementById('userSearch');
    if (searchInput) {
        clearTimeout(searchUsers.timer);
        searchUsers.timer = setTimeout(() => {
            loadUsers(1);
        }, 500);
    }
}

// Startap qidirish
function searchStartups() {
    const searchInput = document.getElementById('startupSearch');
    if (searchInput) {
        clearTimeout(searchStartups.timer);
        searchStartups.timer = setTimeout(() => {
            loadStartups(1);
        }, 500);
    }
}

// Faollik filtrlash
function filterActivity(type) {
    const buttons = document.querySelectorAll('.activity-filter-btn');
    buttons.forEach(btn => btn.classList.remove('active'));
    
    event.target.classList.add('active');
    
    // Bu yerda faollikni filtrlash logikasi bo'ladi
    showToast(`${type === 'all' ? 'Hamma' : type} faollik ko'rsatilmoqda`, 'info');
}

// Admin qo'shish modalini ochish
function openAddAdminModal() {
    openModal('addAdminModal');
}

// Backup yaratish
function createBackup() {
    if (!confirm('Backup yaratilsinmi?')) return;
    
    showToast('Backup yaratilmoqda...', 'info');
    setTimeout(() => {
        showToast('Backup muvaffaqiyatli yaratildi', 'success');
        loadBackups();
    }, 2000);
}

// Xabar yuborish formasi
document.getElementById('broadcastForm')?.addEventListener('submit', async function(e) {
    e.preventDefault();
    
    const message = document.getElementById('messageText').value;
    const recipientType = document.getElementById('messageType').value;
    
    try {
        const response = await fetch('/api/broadcast', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                message,
                recipient_type: recipientType
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            showToast('Xabar yuborildi', 'success');
            this.reset();
            
            // Xabar tarixini yangilash
            addToMessageHistory(data.data);
        } else {
            showToast(data.error || 'Xabar yuborish xatosi', 'error');
        }
    } catch (error) {
        console.error('Xabar yuborish xatosi:', error);
        showToast('Server xatosi', 'error');
    }
});

// Xabar tarixiga qo'shish
function addToMessageHistory(message) {
    const historyList = document.getElementById('messageHistory');
    if (historyList) {
        const messageItem = document.createElement('div');
        messageItem.className = 'history-item';
        messageItem.innerHTML = `
            <div class="history-message">${message.message}</div>
            <div class="history-meta">
                <span>${formatDate(message.sent_at)}</span>
                <span>•</span>
                <span>${message.recipient_type === 'all' ? 'Barcha' : message.recipient_type}</span>
                <span>•</span>
                <span>${message.sent_by}</span>
            </div>
        `;
        
        historyList.insertBefore(messageItem, historyList.firstChild);
    }
}

// Sozlamalarni saqlash
document.getElementById('generalSettingsForm')?.addEventListener('submit', async function(e) {
    e.preventDefault();
    
    const settings = {
        site_name: document.getElementById('siteName').value,
        admin_email: document.getElementById('adminEmail').value,
        timezone: document.getElementById('timezone').value
    };
    
    try {
        const response = await fetch('/api/settings', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(settings)
        });
        
        const data = await response.json();
        
        if (data.success) {
            showToast('Sozlamalar saqlandi', 'success');
        } else {
            showToast(data.error || 'Sozlamalarni saqlash xatosi', 'error');
        }
    } catch (error) {
        console.error('Sozlamalar xatosi:', error);
        showToast('Server xatosi', 'error');
    }
});

// Tab o'zgartirish
document.querySelectorAll('.settings-tab').forEach(tab => {
    tab.addEventListener('click', function() {
        const tabId = this.dataset.tab;
        
        // Tablarni faollashtirish
        document.querySelectorAll('.settings-tab').forEach(t => t.classList.remove('active'));
        this.classList.add('active');
        
        // Kontentni ko'rsatish
        document.querySelectorAll('.settings-tab-content').forEach(content => {
            content.classList.remove('active');
        });
        document.getElementById(tabId + 'Tab')?.classList.add('active');
    });
});

// Sozlamalar yuklanganda
if (currentPage === 'settings') {
    loadSettings();
}

// Modal oynasi tashqarisini bosganda yopish
document.getElementById('modalOverlay')?.addEventListener('click', function(e) {
    if (e.target === this) {
        closeModal();
    }
});