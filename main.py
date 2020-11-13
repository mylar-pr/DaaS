from fastapi import FastAPI
# from app.api.api_v1.api import router as api_router
from fastapi import Security, Depends, FastAPI, HTTPException
from fastapi.security.api_key import APIKeyQuery, APIKeyCookie, APIKeyHeader, APIKey
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from starlette.status import HTTP_403_FORBIDDEN
from starlette.responses import RedirectResponse, JSONResponse
from fastapi_cloudauth.cognito import Cognito, CognitoCurrentUser, CognitoClaims
from mangum import Mangum
import pandas as pd
from datetime import datetime
import boto3
from faker import Factory

# API_KEY = "1234567asdfgh"
# API_KEY_NAME = "access_token"
# COOKIE_DOMAIN = "localtest.me"

# api_key_query = APIKeyQuery(name=API_KEY_NAME, auto_error=False)
# api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)
# api_key_cookie = APIKeyCookie(name=API_KEY_NAME, auto_error=False)

# app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

app = FastAPI()
auth = Cognito(region= "us-east-2", userPoolId="us-east-2_sa8eVZxAM")

get_current_user = CognitoCurrentUser(
    region= "us-east-2", userPoolId="us-east-2_sa8eVZxAM"
)

# async def get_api_key(
#     api_key_query: str = Security(api_key_query),
    # api_key_header: str = Security(api_key_header),
    # api_key_cookie: str = Security(api_key_cookie),
# ):

#     if api_key_query == API_KEY:
#         return api_key_query
    # elif api_key_header == API_KEY:
    #     return api_key_header
    # elif api_key_cookie == API_KEY:
    #     return api_key_cookie
    # else:
    #     raise HTTPException(
    #         status_code=HTTP_403_FORBIDDEN, detail="Could not validate credentials"
    #     )

# api_key: APIKey = Depends(get_api_key)    

@app.get("/", tags=['Homepage'])
async def description(current_user: CognitoClaims = Depends(get_current_user)):
    return {"message": "Data-as-a-service local test environment"}

@app.get("/openapi.json", tags=["Documentation"])
async def get_open_api_endpoint():
    response = JSONResponse(
        get_openapi(title="FastAPI security test", version=1, routes=app.routes)
    )
    return response

@app.get("/docs", tags=["Documentation"])
async def get_documentation():
    response = get_swagger_ui_html(openapi_url="/openapi.json", title="docs")
    response.set_cookie(
        API_KEY_NAME,
        value=api_key,
        domain=COOKIE_DOMAIN,
        httponly=True,
        max_age=1800,
        expires=1800,
    )
    return response

@app.get("/startDate/{start_date}/endDate/{end_date}", tags=['Data-as-a-service API endpoints'])
async def get_startDate_to_endDate(start_date: str, end_date: str, current_user: CognitoClaims = Depends(get_current_user)):
    item = {"start": start_date, "end": end_date}
    
    #Get the dynamodb table
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2', aws_access_key_id='AKIAYUPKUH7UGJPJSMWM', aws_secret_access_key='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')

    table = dynamodb.Table('daas-table')

    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedResponse' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    #create dataframe for dynamo table
    df_dynamo = pd.DataFrame(data)
    df_dynamo['Time'] = pd.to_datetime(df_dynamo['Time'])
    df_dynamo = df_dynamo.sort_values(by=['Time'])
    
    
    start_date = item['start'].split('-')
    end_date = item['end'].split('-')
    y1 = start_date[0]; y2 = end_date[0]
    m1 = start_date[1]; m2 = end_date[1]
    d1 = start_date[2]; d2 = end_date[2]

    df_slice = df_dynamo[(df_dynamo.Time >= datetime(int(y1), int(m1), int(d1))) &
         (df_dynamo.Time <= datetime(int(y2), int(m2), int(d2)))]
    
    df_json = df_slice.to_json()
    
    return df_json

@app.get("/randomSample/{samples}", tags=['Data-as-a-service API endpoints'])
async def get_randomSample(samples: str, current_user: CognitoClaims = Depends(get_current_user)):
    """
    Get n random samples from the dataset.
    """
    #Get the dynamodb table
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2', aws_access_key_id='AKIAYUPKUH7UGJPJSMWM', aws_secret_access_key='ieM2oCnvtvXJZGQ+Vyjw5p/Wfa+gVSYi0ouxvsvY')

    table = dynamodb.Table('daas-table')

    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedResponse' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    #create dataframe for dynamo table
    df_dynamo = pd.DataFrame(data)
    df_dynamo['Time'] = pd.to_datetime(df_dynamo['Time'])
    df_dynamo = df_dynamo.sort_values(by=['Time'])
    
    return df_dynamo.sample(int(samples))

@app.get("/startFeature/{start_feature}/endFeature/{end_feature}", tags=['Data-as-a-service API endpoints'])
async def get_startFeature_to_endFeature(start_feature: str, end_feature: str, current_user: CognitoClaims = Depends(get_current_user)):
    #Get the dynamodb table
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2', aws_access_key_id='AKIAYUPKUH7UGJPJSMWM', aws_secret_access_key='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')

    table = dynamodb.Table('daas-table')

    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedResponse' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    #create dataframe for dynamo table
    df_dynamo = pd.DataFrame(data)
    df_dynamo['Time'] = pd.to_datetime(df_dynamo['Time'])
    df_dynamo = df_dynamo.sort_values(by=['Time'])

    additional_features = []
    for feature in list(df_dynamo.columns):
        if (feature.split(" ")[0][:2] != 'fe'):
            additional_features.append(feature)

    feature_numbers = []
    for feature in list(df_dynamo.columns):
        if (feature.split(" ")[-1][0] == '5'):
            feature_numbers.append(int(feature.split(" ")[-1])) 

    columns = ['feature '+ str(i) for i in range(0,590)]
    df_columns = additional_features.extend([i for i in columns])
    df_columns = additional_features.copy()
    df_dynamo_rearranged = df_dynamo[df_columns]

    columns_to_select = ['Unnamed: 0', 'secomId', 'filename', 'Time', 'load_date']
    columns_to_select.extend(['feature '+ str(i) for i in range(int(start_feature), int(end_feature)+1)])

    return df_dynamo_rearranged[columns_to_select]

@app.get("/feature_by_number/{feature_number}", tags=['Data-as-a-service API endpoints'])
async def get_feature_by_number(feature_number: str, current_user: CognitoClaims = Depends(get_current_user)):
    #Get the dynamodb table
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2', aws_access_key_id='AKIAYUPKUH7UGJPJSMWM', aws_secret_access_key='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')

    table = dynamodb.Table('daas-table')

    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedResponse' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    #create dataframe for dynamo table
    df_dynamo = pd.DataFrame(data)
    df_dynamo['Time'] = pd.to_datetime(df_dynamo['Time'])
    df_dynamo = df_dynamo.sort_values(by=['Time'])
    
    return df_dynamo['feature '+feature_number]


@app.get("/randomAnonymizedSample/{samples}", tags=['Data-as-a-service API endpoints'])
async def get_AnonymizedSample(samples: str, current_user: CognitoClaims = Depends(get_current_user)):
    """
    Get n random samples from the dataset with date & time anonymized.
    """
    #Get the dynamodb table
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2', aws_access_key_id='AKIAYUPKUH7UGJPJSMWM', aws_secret_access_key='ieM2oCnvtvXJZGQ+Vyjw5p/Wfa+gVSYi0ouxvsvY')

    table = dynamodb.Table('daas-table')
    fake = Factory.create()
    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedResponse' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    #create dataframe for dynamo table
    df_dynamo = pd.DataFrame(data)
    df_dynamo['Time'] = pd.to_datetime(df_dynamo['Time'])
    df_dynamo = df_dynamo.sort_values(by=['Time'])
    
    #sampling and anonymizing the data
    df_dynamo = df_dynamo.sample(int(samples))
    df_dynamo["Time"] = [fake.time() + " " + fake.date() for i in range(len(df_dynamo))]
    return df_dynamo.sample(int(samples))


@app.get("/userName/{user_name}/token/{token}", tags=['Data-as-a-service API endpoints'])
async def validateToken(token: str, current_user: CognitoClaims = Depends(get_current_user)):
    """
    Validate token API:
    checks if the token entered with a valid username, is valid or not.
    inputs: user name , token id
    output: json format {result: 1 or 0,
                        message: "message"}
    """
    dynamodb = boto3.resource('dynamodb', 
                              region_name='us-east-2', 
                              aws_access_key_id='AKIAYUPKUH7UGJPJSMWM', 
                              aws_secret_access_key='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')
    
    table = dynamodb.Table('token-table')

    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedResponse' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    #create dataframe for dynamo table
    df_dynamo = pd.DataFrame(data)
    
    #check if token is present in the table. If true, return username saying user is valid.
    if token in list(df_dynamo["token"]):
        user_name = df_dynamo.loc[df_dynamo['token'] == token]["userName"]
        # print(str(user_name))
        print("Token is valid")
        return {"result" : 1,
                   "message" : "Token is valid for username - {}".format(user_name.values[0])}
    else:
        return {"result" : 0,
                "message" : "Token is not valid."}

    # #check if userName is present in the table. If true, check if the token w.r.t. to that user is valid.
    # if user_name in list(df_dynamo["userName"]):
    #     if (df_dynamo.loc[df_dynamo['userName'] == user_name]["token"] == token).all():
    #         print(df_dynamo.loc[df_dynamo['userName'] == user_name]["token"] == token)
    #         print("Token is valid")
    #         return {"result" : 1,
    #                "message" : "Token is valid"}
    #     else:
    #         return {"result" : 0,
    #                "message" : "Token is not valid."}
    # else:
    #     print("No such username found.")
    #     return {"result" : 0,
    #             "message" : "Username not found. Please ensure you entered a correct username."}
    



# app.include_router(api_router, prefix="/api/v1")
handler = Mangum(app)

