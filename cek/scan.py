import asyncio
import aiohttp
import os
import time
from datetime import datetime

# Path otomatis
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
IP_FILE = os.path.join(BASE_DIR, 'file.txt')
OUTPUT_ACTIVE = os.path.join(BASE_DIR, 'proxyList.txt')
OUTPUT_DEAD = os.path.join(BASE_DIR, 'dead.txt')
API_URL = 'https://check.jak.biz.id/check?ip={ip}:{port}'

# Limit simultan (100-200 aman untuk GitHub Actions)
CONCURRENT_LIMIT = 150 

# Set untuk tracking IP:Port yang sudah diproses
processed_proxies = set()

async def check_proxy(session, p, semaphore):
    ip, port = p['ip'], p['port']
    url = API_URL.format(ip=ip, port=port)
    
    async with semaphore:
        try:
            # Timeout diperketat ke 7 detik agar tidak terlalu lama menggantung
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=7)) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('status', '').upper() == 'ACTIVE':
                        delay = data.get('delay', 'N/A')
                        print(f"✅ {ip}:{port} | {delay}", flush=True)
                        return True, p, delay
                
                # Jika tidak aktif (tapi respon 200) atau status bukan ACTIVE
                print(f"❌ {ip}:{port} | Status: {response.status}", flush=True)
                return False, p, None
        except Exception:
            # Print minimal agar log tetap berjalan meski error/timeout
            print(f"❌ {ip}:{port} | Timeout/Error", flush=True)
            return False, p, None

def read_proxies():
    proxies = []
    processed_proxies.clear()  # Reset set setiap kali baca
    
    if not os.path.exists(IP_FILE):
        return []
    
    with open(IP_FILE, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                parts = line.split(',')
                if len(parts) >= 2:
                    ip = parts[0].strip()
                    port = parts[1].strip()
                    proxy_key = f"{ip}:{port}"
                    
                    # Skip jika IP:Port sudah diproses
                    if proxy_key in processed_proxies:
                        print(f"⚠️ SKIP DUPLIKAT: {proxy_key}", flush=True)
                        continue
                    
                    processed_proxies.add(proxy_key)
                    
                    proxies.append({
                        'ip': ip,
                        'port': port,
                        'country': parts[2].strip() if len(parts) > 2 else 'Unknown',
                        'isp': parts[3].strip() if len(parts) > 3 else 'Unknown',
                    })
    
    print(f"📋 Total unique proxy setelah filter duplikat: {len(proxies)}", flush=True)
    return proxies

def merge_with_existing_results(new_active, new_dead):
    """Merge hasil baru dengan hasil yang sudah ada (menghindari duplikat di output final)"""
    
    # Baca hasil yang sudah ada (jika ada)
    existing_active = set()
    existing_dead = set()
    
    if os.path.exists(OUTPUT_ACTIVE):
        with open(OUTPUT_ACTIVE, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    # Ambil IP:Port dari baris (3 field pertama)
                    parts = line.split(',')
                    if len(parts) >= 2:
                        existing_active.add(f"{parts[0]}:{parts[1]}")
    
    if os.path.exists(OUTPUT_DEAD):
        with open(OUTPUT_DEAD, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    parts = line.split(',')
                    if len(parts) >= 2:
                        existing_dead.add(f"{parts[0]}:{parts[1]}")
    
    # Filter hasil baru menghindari duplikat dengan hasil lama
    unique_active = []
    for line in new_active:
        parts = line.split(',')
        if len(parts) >= 2:
            proxy_key = f"{parts[0]}:{parts[1]}"
            if proxy_key not in existing_active and proxy_key not in existing_dead:
                unique_active.append(line)
            else:
                print(f"⚠️ SKIP DUPLIKAT DENGAN HASIL LAMA: {proxy_key}", flush=True)
    
    unique_dead = []
    for line in new_dead:
        parts = line.split(',')
        if len(parts) >= 2:
            proxy_key = f"{parts[0]}:{parts[1]}"
            if proxy_key not in existing_active and proxy_key not in existing_dead:
                unique_dead.append(line)
            else:
                print(f"⚠️ SKIP DUPLIKAT DENGAN HASIL LAMA: {proxy_key}", flush=True)
    
    return unique_active, unique_dead

def save_results(active_results, dead_results):
    """Simpan hasil dengan merge ke file yang sudah ada (append jika tidak duplikat)"""
    
    # Merge dengan hasil yang sudah ada
    final_active, final_dead = merge_with_existing_results(active_results, dead_results)
    
    # Append ke file yang sudah ada (tanpa duplikat)
    if final_active:
        with open(OUTPUT_ACTIVE, 'a') as f:
            for line in final_active:
                f.write(line + '\n')
    
    if final_dead:
        with open(OUTPUT_DEAD, 'a') as f:
            for line in final_dead:
                f.write(line + '\n')
    
    return len(final_active), len(final_dead)

async def main():
    print("="*50, flush=True)
    print(f"🚀 STARTING SCANNER - {datetime.now().strftime('%H:%M:%S')}", flush=True)
    print("="*50, flush=True)
    
    proxies = read_proxies()
    if not proxies:
        print("❌ File sumber kosong!", flush=True)
        return

    semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)
    
    # Optimasi: Matikan verifikasi SSL & gunakan DNS cache agar tidak 'stuck'
    connector = aiohttp.TCPConnector(limit=CONCURRENT_LIMIT, ssl=False, use_dns_cache=True)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [check_proxy(session, p, semaphore) for p in proxies]
        results = await asyncio.gather(*tasks)

        active_results = []
        dead_results = []
        
        for is_alive, p, delay in results:
            line = f"{p['ip']},{p['port']},{p['country']},{p['isp']}"
            if is_alive:
                active_results.append(line)
            else:
                dead_results.append(line)

    # Simpan hasil dengan menghindari duplikat
    new_active_count, new_dead_count = save_results(active_results, dead_results)

    print("\n" + "="*50, flush=True)
    print(f"📊 HASIL SCAN INI: ✅ {len(active_results)} | ❌ {len(dead_results)}", flush=True)
    print(f"📝 YANG DISIMPAN (BANYAK): ✅ {new_active_count} | ❌ {new_dead_count}", flush=True)
    print("="*50, flush=True)

if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    print(f"⏱️ Selesai dalam {time.time() - start_time:.2f} detik", flush=True)
