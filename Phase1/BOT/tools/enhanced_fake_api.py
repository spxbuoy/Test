import httpx
import json
import random
import traceback
from typing import Dict, List, Optional, Tuple, Union
from datetime import datetime, timedelta

# Import database functions
from FUNC.defs import error_log
from BOT.admin.api_management import get_api_key, get_schema_id, increment_api_usage, update_api_success_rate

# Cache storage for API responses (in-memory, will reset on bot restart)
API_CACHE = {}

async def get_data_from_mockaroo(country_code: str) -> Tuple[bool, Union[Dict, str]]:
    """Get fake data from Mockaroo API based on country."""
    try:
        # Check cache first
        cache_key = f"mockaroo_{country_code}"
        if cache_key in API_CACHE:
            cache_entry = API_CACHE[cache_key]
            # Use cache if it's less than 24 hours old
            if datetime.now() - cache_entry["timestamp"] < timedelta(hours=24):
                # Return a random entry from the cached batch
                return True, random.choice(cache_entry["data"])
        
        # Get API key
        api_key = await get_api_key("mockaroo")
        if not api_key:
            return False, "Mockaroo API key not set or disabled"
        
        # Get schema ID for this country
        schema_id = await get_schema_id("mockaroo", country_code)
        
        # If no specific schema, try default
        if not schema_id:
            schema_id = await get_schema_id("mockaroo", "default")
            
        # If still no schema, use direct parameters
        if not schema_id:
            url = f"https://api.mockaroo.com/api/generate.json?key={api_key}"
            params = {
                "count": 10,  # Get 10 entries to cache
                "fields[0][name]": "first_name",
                "fields[0][type]": "First Name",
                "fields[1][name]": "last_name",
                "fields[1][type]": "Last Name",
                "fields[2][name]": "gender",
                "fields[2][type]": "Gender",
                "fields[3][name]": "street_address",
                "fields[3][type]": "Street Address",
                "fields[4][name]": "city",
                "fields[4][type]": "City",
                "fields[5][name]": "state",
                "fields[5][type]": "State",
                "fields[6][name]": "postal_code",
                "fields[6][type]": "Postal Code",
                "fields[7][name]": "country",
                "fields[7][type]": "Country",
                "fields[8][name]": "phone",
                "fields[8][type]": "Phone",
                "fields[9][name]": "email",
                "fields[9][type]": "Email"
            }
        else:
            # Use the schema ID
            url = f"https://api.mockaroo.com/api/{schema_id}.json?key={api_key}&count=10"
            params = {}
        
        # Make the API request
        async with httpx.AsyncClient() as session:
            if params:
                response = await session.get(url, params=params)
            else:
                response = await session.get(url)
            
            # Check response
            if response.status_code == 200:
                data = response.json()
                
                # Cache the result
                API_CACHE[cache_key] = {
                    "timestamp": datetime.now(),
                    "data": data
                }
                
                # Increment usage count
                await increment_api_usage("mockaroo")
                
                # Return one random entry
                return True, random.choice(data)
            else:
                # Update failure rate
                await update_api_success_rate("mockaroo", False)
                return False, f"Mockaroo API returned status code {response.status_code}: {response.text}"
                
    except Exception as e:
        # Log the error and update failure rate
        await error_log(traceback.format_exc())
        await update_api_success_rate("mockaroo", False)
        return False, str(e)
        
async def get_data_from_randomapi(country_code: str) -> Tuple[bool, Union[Dict, str]]:
    """Get fake data from RandomAPI.com based on country."""
    try:
        # Check cache first
        cache_key = f"randomapi_{country_code}"
        if cache_key in API_CACHE:
            cache_entry = API_CACHE[cache_key]
            # Use cache if it's less than 24 hours old
            if datetime.now() - cache_entry["timestamp"] < timedelta(hours=24):
                # Return a random entry from the cached batch
                return True, random.choice(cache_entry["data"])
        
        # Get API key
        api_key = await get_api_key("randomapi")
        if not api_key:
            return False, "RandomAPI key not set or disabled"
        
        # Get API ID for this country
        api_id = await get_schema_id("randomapi", country_code)
        
        # If no specific API ID, try default
        if not api_id:
            api_id = await get_schema_id("randomapi", "default")
            
        # If still no API ID, return error
        if not api_id:
            return False, "No RandomAPI schema configured for this country"
        
        # Make the API request
        async with httpx.AsyncClient() as session:
            params = {
                "key": api_key,
                "id": api_id,
                "results": 10,  # Get 10 entries to cache
                "noinfo": "true",
                "fmt": "json"
            }
            
            url = "https://randomapi.com/api"
            response = await session.get(url, params=params)
            
            # Check response
            if response.status_code == 200:
                data = response.json().get("results", [])
                
                # Check if we have valid data
                if not data or len(data) == 0:
                    await update_api_success_rate("randomapi", False)
                    return False, "RandomAPI returned empty results"
                
                # Cache the result
                API_CACHE[cache_key] = {
                    "timestamp": datetime.now(),
                    "data": data
                }
                
                # Increment usage count
                await increment_api_usage("randomapi")
                
                # Return one random entry
                return True, random.choice(data)
            else:
                # Update failure rate
                await update_api_success_rate("randomapi", False)
                return False, f"RandomAPI returned status code {response.status_code}: {response.text}"
                
    except Exception as e:
        # Log the error and update failure rate
        await error_log(traceback.format_exc())
        await update_api_success_rate("randomapi", False)
        return False, str(e)

async def get_data_from_randomuser(country_code: str) -> Tuple[bool, Union[Dict, str]]:
    """Get fake data from RandomUser.me API based on country."""
    try:
        # Check cache first
        cache_key = f"randomuser_{country_code}"
        if cache_key in API_CACHE:
            cache_entry = API_CACHE[cache_key]
            # Use cache if it's less than 6 hours old
            if datetime.now() - cache_entry["timestamp"] < timedelta(hours=6):
                # Return a random entry from the cached batch
                return True, random.choice(cache_entry["data"])
        
        # Map of country codes for RandomUser API
        # RandomUser only supports a limited set of nationalities
        supported_nat = {
            'au': 'au', 'br': 'br', 'ca': 'ca', 'ch': 'ch', 'de': 'de', 
            'dk': 'dk', 'es': 'es', 'fi': 'fi', 'fr': 'fr', 'gb': 'gb', 
            'uk': 'gb', 'ie': 'ie', 'in': 'in', 'ir': 'ir', 'mx': 'mx', 
            'nl': 'nl', 'no': 'no', 'nz': 'nz', 'rs': 'rs', 'tr': 'tr', 
            'ua': 'ua', 'us': 'us'
        }
        
        # Check if country is supported
        nat = supported_nat.get(country_code)
        if not nat:
            # Try a region-based fallback
            region_map = {
                # European countries fall back to GB
                'it': 'gb', 'pl': 'gb', 'se': 'gb', 'cz': 'gb', 'hu': 'gb',
                # Asian countries fall back to IN
                'jp': 'in', 'cn': 'in', 'kr': 'in', 'th': 'in', 'vn': 'in',
                # Middle Eastern countries fall back to IR
                'sa': 'ir', 'ae': 'ir', 'il': 'ir',
                # Default to US for others
            }
            nat = region_map.get(country_code, 'us')
        
        # Make the API request
        async with httpx.AsyncClient() as session:
            params = {
                "nat": nat,
                "results": 10  # Get 10 entries to cache
            }
            
            url = "https://randomuser.me/api/"
            response = await session.get(url, params=params)
            
            # Check response
            if response.status_code == 200:
                data = response.json().get("results", [])
                
                # Check if we have valid data
                if not data or len(data) == 0:
                    await update_api_success_rate("randomuser", False)
                    return False, "RandomUser API returned empty results"
                
                # Cache the result
                API_CACHE[cache_key] = {
                    "timestamp": datetime.now(),
                    "data": data
                }
                
                # Increment usage count
                await increment_api_usage("randomuser")
                
                # Return one random entry
                return True, random.choice(data)
            else:
                # Update failure rate
                await update_api_success_rate("randomuser", False)
                return False, f"RandomUser API returned status code {response.status_code}: {response.text}"
                
    except Exception as e:
        # Log the error and update failure rate
        await error_log(traceback.format_exc())
        await update_api_success_rate("randomuser", False)
        return False, str(e)

async def get_best_fake_data(country_code: str) -> Tuple[bool, Union[Dict, str], str]:
    """Try multiple APIs to get the best fake data for a country."""
    # Get API keys to determine which services are available
    mockaroo_api = await get_api_key("mockaroo")
    randomapi_api = await get_api_key("randomapi")
    
    # Define the order of APIs to try
    apis_to_try = []
    
    # Only add APIs that are configured
    if mockaroo_api:
        apis_to_try.append(("mockaroo", get_data_from_mockaroo))
    if randomapi_api:
        apis_to_try.append(("randomapi", get_data_from_randomapi))
    
    # Always add RandomUser.me as a fallback
    apis_to_try.append(("randomuser", get_data_from_randomuser))
    
    # Try each API in order
    last_data = {}
    last_api = "randomuser"
    
    for api_name, api_func in apis_to_try:
        success, data = await api_func(country_code)
        last_data = data
        last_api = api_name
        if success:
            return True, data, api_name
    
    # If all APIs failed, return the last error
    return False, last_data, last_api

async def test_api_request(api_name: str, country_code: str) -> Tuple[bool, Union[Dict, str]]:
    """Test a specific API for admin purposes."""
    if api_name == "mockaroo":
        return await get_data_from_mockaroo(country_code)
    elif api_name == "randomapi":
        return await get_data_from_randomapi(country_code)
    elif api_name == "randomuser":
        return await get_data_from_randomuser(country_code)
    else:
        return False, f"Unknown API: {api_name}"

def clear_api_cache(api_name=None, country_code=None):
    """Clear the API cache, optionally filtering by API name and/or country code."""
    global API_CACHE
    
    if api_name and country_code:
        # Clear specific cache entry
        cache_key = f"{api_name}_{country_code}"
        if cache_key in API_CACHE:
            del API_CACHE[cache_key]
    elif api_name:
        # Clear all entries for this API
        keys_to_delete = [k for k in API_CACHE.keys() if k.startswith(f"{api_name}_")]
        for key in keys_to_delete:
            del API_CACHE[key]
    elif country_code:
        # Clear all entries for this country
        keys_to_delete = [k for k in API_CACHE.keys() if k.endswith(f"_{country_code}")]
        for key in keys_to_delete:
            del API_CACHE[key]
    else:
        # Clear all cache
        API_CACHE = {}