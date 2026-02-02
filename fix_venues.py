import os
from supabase import create_client

with open('/Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM/.streamlit/secrets.toml', 'r') as f:
    content = f.read()
    
for line in content.split('\n'):
    if 'SUPABASE_URL' in line and '=' in line:
        url = line.split('=')[1].strip().strip('"')
    if 'SUPABASE_KEY' in line and '=' in line:
        key = line.split('=')[1].strip().strip('"')

supabase = create_client(url, key)

# Disable all except Moderna museet
print("Fixing venue enabled status...")
response = supabase.table('scraping_urls').select('*').execute()
for u in response.data:
    if u['name'] == 'Moderna museet':
        supabase.table('scraping_urls').update({'enabled': 1}).eq('id', u['id']).execute()
        print(f"  Enabled: {u['name']}")
    else:
        supabase.table('scraping_urls').update({'enabled': 0}).eq('id', u['id']).execute()
        print(f"  Disabled: {u['name']}")

print("\nDone! Now only Moderna museet is enabled.")
