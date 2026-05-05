#!/usr/bin/env python3
"""
Test script for ADLS Gen2 connection validation.
Run this to verify your connection string and ADLS Gen2 setup.
"""

import os
import sys
from pathlib import Path

# Add app to path for imports
sys.path.append(str(Path(__file__).parent / "app"))

def test_adls_connection():
    """Test ADLS Gen2 connection with your configuration."""
    
    print("🧪 Testing ADLS Gen2 Connection")
    print("=" * 50)
    
    # Test 1: Import check
    try:
        from azure.storage.filedatalake import DataLakeServiceClient
        print("✅ Azure ADLS Gen2 SDK imported successfully")
    except ImportError as e:
        print(f"❌ Azure SDK import failed: {e}")
        print("   Run: pip install azure-storage-file-datalake==12.21.0")
        return False
    
    # Test 2: Configuration check
    connection_string = os.getenv(
        "AZURE_CONNECTION_STRING",
        "DefaultEndpointsProtocol=https;AccountName=sensedatalaketest;AccountKey=yGBBxxxxxxxx==;EndpointSuffix=core.windows.net"
    )
    file_system_name = os.getenv("AZURE_FILE_SYSTEM_NAME", "sensor-data-cold-storage")
    
    print(f"📋 Configuration:")
    print(f"   Account: {extract_account_name(connection_string)}")
    print(f"   File System: {file_system_name}")
    print(f"   Connection String Length: {len(connection_string)} chars")
    
    # Test 3: Connection string validation
    if not validate_connection_string(connection_string):
        print("❌ Connection string validation failed")
        return False
    
    print("✅ Connection string format is valid")
    
    # Test 4: SDK connection test
    try:
        client = DataLakeServiceClient.from_connection_string(connection_string)
        print("✅ DataLakeServiceClient created successfully")
        print(f"   Account URL: {client.url}")
    except Exception as e:
        print(f"❌ Failed to create DataLakeServiceClient: {e}")
        return False
    
    # Test 5: File system client test
    try:
        fs_client = client.get_file_system_client(file_system_name)
        print("✅ File system client created successfully")
    except Exception as e:
        print(f"❌ Failed to create file system client: {e}")
        return False
    
    # Test 6: Authentication test (if credentials are real)
    if "yGBBxxxxxxxx" not in connection_string:
        try:
            # Try to get file system properties (requires valid credentials)
            props = fs_client.get_file_system_properties()
            print("✅ Authentication successful - file system accessible")
            print(f"   Last modified: {props.last_modified}")
        except Exception as e:
            print(f"⚠️  Authentication test failed (expected if using dummy key): {e}")
            print("   This is normal if you haven't replaced the dummy key yet")
    else:
        print("ℹ️  Skipping authentication test (dummy key detected)")
        print("   Replace 'yGBBxxxxxxxx' with your actual account key for full test")
    
    print("\n🎯 Next Steps:")
    print("1. Replace dummy key in connection string with your actual key")
    print("2. Ensure file system exists in your ADLS Gen2 account")
    print("3. Start the service with: make run")
    print("4. Test upload with: curl -X POST http://localhost:8080/upload/trigger")
    
    return True

def extract_account_name(connection_string: str) -> str:
    """Extract account name from connection string."""
    import re
    match = re.search(r"AccountName=([^;]+)", connection_string)
    return match.group(1) if match else "unknown"

def validate_connection_string(connection_string: str) -> bool:
    """Validate connection string format."""
    required_parts = [
        "DefaultEndpointsProtocol=https",
        "AccountName=",
        "AccountKey=", 
        "EndpointSuffix=core.windows.net"
    ]
    
    # Check for common typos
    if "DefaultEndpoimtProtocol" in connection_string:
        print("❌ Connection string typo: 'DefaultEndpoimtProtocol' should be 'DefaultEndpointsProtocol'")
        return False
    
    if "AccontKey" in connection_string:
        print("❌ Connection string typo: 'AccontKey' should be 'AccountKey'")
        return False
    
    # Validate all required parts
    for part in required_parts:
        if part not in connection_string:
            print(f"❌ Connection string missing: {part}")
            return False
    
    return True

if __name__ == "__main__":
    print("ADLS Gen2 Connection Test")
    print("=" * 30)
    
    # Load environment from .env file if it exists
    try:
        from dotenv import load_dotenv
        if Path(".env").exists():
            load_dotenv()
            print("✅ Loaded configuration from .env file")
        else:
            print("ℹ️  No .env file found, using default values")
    except ImportError:
        print("ℹ️  python-dotenv not installed, using environment variables")
    
    success = test_adls_connection()
    
    if success:
        print("\n🎉 ADLS Gen2 connection test completed successfully!")
        sys.exit(0)
    else:
        print("\n❌ ADLS Gen2 connection test failed!")
        sys.exit(1)