import re
import sys

sys.stdout.reconfigure(encoding='utf-8')

with open('app/backend/bot.py', encoding='utf-8') as f:
    content = f.read()

cb_pattern = re.compile(r'"callback_data":\s*f?["\']([^"\']+)["\']')
matches = cb_pattern.findall(content)

extracted = set(matches)
cb_handler_section = content[content.find('async def handle_bot_callback'):content.find('async def process_bot_callback_task')]

print(f"Found {len(extracted)} distinct callback_data patterns:")
unhandled = []
for cb in sorted(extracted):
    base = cb.split('{')[0] if '{' in cb else cb
    if base in cb_handler_section or base.rstrip('_') in cb_handler_section:
        status = "✅ OK"
    else:
        status = "❌ UNHANDLED"
        unhandled.append(cb)
    print(f"  {status:15} -> {cb}")

print(f"\nTotal unhandled or mismatched callback patterns: {len(unhandled)}")
for u in unhandled:
    print(f"  • {u}")
