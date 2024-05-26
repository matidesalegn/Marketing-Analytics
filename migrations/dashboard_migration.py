import requests

def migrate_dashboard_queries(source_api_key, destination_api_key):
    # Fetch queries from source Redash instance
    headers = {'Authorization': f'Key {source_api_key}'}
    source_queries = requests.get('https://source-redash-instance/api/queries', headers=headers).json()

    # Migrate queries to destination Redash instance
    headers = {'Authorization': f'Key {destination_api_key}', 'Content-Type': 'application/json'}
    for query in source_queries:
        query_data = {
            'name': query['name'],
            'query': query['query'],
            'data_source_id': query['data_source_id']
        }
        response = requests.post('https://destination-redash-instance/api/queries', headers=headers, json=query_data)
        if response.status_code == 200:
            print(f"Query '{query['name']}' migrated successfully.")
        else:
            print(f"Failed to migrate query '{query['name']}'.")

def migrate_dashboards(source_api_key, destination_api_key):
    # Fetch dashboards from source Redash instance
    headers = {'Authorization': f'Key {source_api_key}'}
    source_dashboards = requests.get('https://source-redash-instance/api/dashboards', headers=headers).json()

    # Migrate dashboards to destination Redash instance
    headers = {'Authorization': f'Key {destination_api_key}', 'Content-Type': 'application/json'}
    for dashboard in source_dashboards:
        dashboard_data = {
            'name': dashboard['name'],
            'layout': dashboard['layout'],
            'widgets': dashboard['widgets']
        }
        response = requests.post('https://destination-redash-instance/api/dashboards', headers=headers, json=dashboard_data)
        if response.status_code == 200:
            print(f"Dashboard '{dashboard['name']}' migrated successfully.")
        else:
            print(f"Failed to migrate dashboard '{dashboard['name']}'.")

# Example usage
migrate_dashboard_queries(
    source_api_key='source_api_key',
    destination_api_key='destination_api_key'
)

migrate_dashboards(
    source_api_key='source_api_key',
    destination_api_key='destination_api_key'
)