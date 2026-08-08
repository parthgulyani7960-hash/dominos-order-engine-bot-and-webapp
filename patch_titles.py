import sys
sys.stdout.reconfigure(encoding='utf-8')

js = open('app/frontend/admin/admin.js', encoding='utf-8').read()

# Fix titles map to add robot
old_titles = "    settings: 'Settings', logs: 'Logs', proxies: 'Proxy Manager'\n  };"
new_titles = "    settings: 'Settings', logs: 'Logs', proxies: 'Proxy Manager', robot: 'Robot Live'\n  };"

if old_titles in js:
    js = js.replace(old_titles, new_titles)
    print('titles map patched')
else:
    print('titles map not found - skipping')

open('app/frontend/admin/admin.js', 'w', encoding='utf-8').write(js)
print('Done')
