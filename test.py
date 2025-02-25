import airbyte_api
from airbyte_api import api, models
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Access the variables
password = os.getenv("password")
name = os.getenv("name")

#How to manipulate source 
#Create source: A workspace in Airbyte is an isolated environment where you manage data integrations, including sources, destinations, connections, and configurations. 
# It acts as a container for your data pipeline setup.
#Think of a workspace as a project folder that contains everything related to your data integration setup..
s = airbyte_api.AirbyteAPI(
    server_url='http://localhost:8000/api/public/v1',
    security=models.Security(
        basic_auth=models.SchemeBasicAuth(
            password=password,
            username=name,
        ),
    ),
)
"""# create a new workspace named "Internship".
res = s.workspaces.create_workspace(request=models.WorkspaceCreateRequest(
    name='Internship',
))

if res.workspace_response is not None:
    # Handle the response (e.g., print workspace details)
    print(res.workspace_response)

"""




"""
#List All Workspaces    
res = s.workspaces.list_workspaces(request=api.ListWorkspacesRequest())

if res.workspaces_response is not None:
    print( res)"""

"""#Get Workspace Details
res = s.workspaces.get_workspace(request=api.GetWorkspaceRequest(
    workspace_id='bfffc95a-a442-4c98-9374-c23bd5fe5997',
))

if res.workspace_response is not None:
    print(res.workspace_response)
"""
"""#Update workspace 
res = s.workspaces.update_workspace(request=api.UpdateWorkspaceRequest(
    workspace_update_request=models.WorkspaceUpdateRequest(
        name='Internship1',
    ),
    workspace_id='bfffc95a-a442-4c98-9374-c23bd5fe5997',
))

"""

"""#Delete a Workspace
res = s.workspaces.delete_workspace(request=api.DeleteWorkspaceRequest(
    workspace_id='bfffc95a-a442-4c98-9374-c23bd5fe5997',
))
"""
# now I will complete with workspace named 'internship' with workspaceid 'eaeb6b57-602d-44a8-8bf1-1b1f99ba08f7'

"""#some confihuration mouch thahrin ena l ghalet wela heya ekeka
# Creating a PostgreSQL source
res = s.sources.create_source(request=models.SourceCreateRequest(
    configuration=models.SourcePostgres(
        host='host.docker.internal',
        port=5432,
        database='mydatabase',
        username='user',
        schemas =["public"],
        password='password',

    ),
    name='PostgreSQL Source 2',
    workspace_id='12112ce2-5757-49b0-b1f5-29835302a67d',
))

if res.source_response is not None:
    print(res)
    print("PostgreSQL source created successfully!")
"""

"""#list all the sources
res = s.sources.list_sources(request=api.ListSourcesRequest(
    workspace_ids=['c80ad9e9-276a-4240-8cb8-1a0aecfe4cc0'],
))

# Check if the response contains data
if res.sources_response and res.sources_response.data:
    # Iterate through each source and print details
    for source in res.sources_response.data:
        print("Source ID:", source.source_id)
        print("Name:", source.name)
        print("Type:", source.source_type)
        print("Workspace ID:", source.workspace_id)
        print("Configuration:", source.configuration)
        print("-" * 40)  # Separator for readability
else:
    print("No sources found.")
"""
"""
#Delet a source
res =  s.sources.delete_source(request=api.DeleteSourceRequest(
    source_id='eaeb6b57-602d-44a8-8bf1-1b1f99ba08f7',
))
"""

"""# get source details

res = s.sources.get_source(request=api.GetSourceRequest(
    source_id='<value>',
))
"""
"""
#Create destination
res = s.destinations.list_destinations(request=api.ListDestinationsRequest())

if res.destinations_response is not None:
    # handle response
    print(res)
"""


"""
res = s.destinations.create_destination(
    request=models.DestinationCreateRequest(
        configuration=models.DestinationPostgres(
            # Connection parameters directly in configuration
            host="host.docker.internal",
            port=5433,
            database="mydatabase",
            username="user",
            password="password",  
            schema="public",
            # Other optional parameters...
        ),
        name="Postgres Destination",
        workspace_id="12112ce2-5757-49b0-b1f5-29835302a67d",
    )
)

if res.destination_response is not None:

    # handle response
    print(res)"""

"""res = s.destinations.list_destinations(request=api.ListDestinationsRequest())

if res.destinations_response is not None:
    # handle response
    print(res)"""

"""#now this is the id of destiniation 42f7138b-b44f-4779-8a24-058d84c4ce6f
#this is the id of source cc7d97ca-e0a6-474f-8d02-1f82e81a10e4
#this it the workspace id  eaeb6b57-602d-44a8-8bf1-1b1f99ba08f7
# Create connection with proper StreamConfiguration structure
# Create connection with string-based sync modes
connection = s.connections.create_connection(
    request=models.ConnectionCreateRequest(
        name="Postgres Connection",
        source_id="2edeabf8-217d-4a3b-a4e5-90bdf0b793c1",
        destination_id="d6f265e7-70c5-4623-9b4b-331c831d298f",
        workspace_id="12112ce2-5757-49b0-b1f5-29835302a67d",
        configurations=models.StreamConfiguration(
            [
                models.StreamConfiguration(
                    name="example_stream",
                    sync_mode="full_refresh_overwrite",
                   
                )
            ]
        ),
        schedule=models.AirbyteAPIConnectionSchedule(
            schedule_type="manual",
           
        ),
       
    )
)
print(f"Created connection ID: {connection}")


"""

"""
#sync
res = s.jobs.create_job(request=models.JobCreateRequest(
    connection_id='5c7f26a5-62c6-471e-9fce-4812b51c9cd2',
    job_type=models.JobTypeEnum.SYNC,
))

if res.job_response is not None:
    # handle response
    pass


"""







