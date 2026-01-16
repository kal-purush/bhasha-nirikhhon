#!/usr/bin/env python3
"""
Test client for the Name Uniqueness API
Run this script to test the API endpoints
"""

import json
import sys

import requests

API_BASE_URL = "http://localhost:5001/api"

def test_health_check():
    """Test the health check endpoint"""
    print("\n=== Testing Health Check ===")
    try:
        response = requests.get(f"{API_BASE_URL}")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.status_code == 200
    except Exception as e:
        print(f"Error: {e}")
        return False

def test_score_name():
    """Test the score-name endpoint"""
    print("\n=== Testing Score Name ===")
    
    test_cases = [
        {"firstName": "John", "lastName": "Smith"},
        {"firstName": "Luna", "lastName": ""},
        {"firstName": "", "lastName": "Zhang"},
    ]
    
    success = True
    
    for test_case in test_cases:
        try:
            print(f"\nTesting with: {test_case}")
            response = requests.post(f"{API_BASE_URL}/score-name", json=test_case)
            print(f"Status Code: {response.status_code}")
            print(f"Response: {json.dumps(response.json(), indent=2)}")
            success = success and response.status_code == 200
        except Exception as e:
            print(f"Error: {e}")
            success = False
    
    return success

def test_compare_names():
    """Test the compare-names endpoint"""
    print("\n=== Testing Compare Names ===")
    
    test_case = {
        "names": [
            ["John", "Smith"],
            ["Luna", "Zhang"],
            ["Zephyr", ""]
        ]
    }
    
    try:
        print(f"\nTesting with: {test_case}")
        response = requests.post(f"{API_BASE_URL}/compare-names", json=test_case)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.status_code == 200
    except Exception as e:
        print(f"Error: {e}")
        return False

def main():
    """Run all tests"""
    print("Starting API tests...")
    
    health_check_success = test_health_check()
    score_name_success = test_score_name()
    compare_names_success = test_compare_names()
    
    print("\n=== Test Results ===")
    print(f"Health Check: {'✅ PASS' if health_check_success else '❌ FAIL'}")
    print(f"Score Name: {'✅ PASS' if score_name_success else '❌ FAIL'}")
    print(f"Compare Names: {'✅ PASS' if compare_names_success else '❌ FAIL'}")
    
    if health_check_success and score_name_success and compare_names_success:
        print("\n🎉 All tests passed! The API is working correctly.")
        return 0
    else:
        print("\n❌ Some tests failed. Please check the API.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 