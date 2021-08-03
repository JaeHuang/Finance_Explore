# replace PROJECT_ID with my project id
# replace NAME with account name
# replace FILE_NAME with output key file name

# switch to the project
gcloud config set project PROJECT_ID

# create service account for the app
gcloud iam service-accounts create NAME

# binding the account to the project under the specific role
gcloud projects add-iam-policy-binding PROJECT_ID --member="serviceAccount:NAME@PROJECT_ID.iam.gserviceaccount.com" --role="roles/owner"

# create the key for the service account
gcloud iam service-accounts keys create FILE_NAME.json --iam-account=NAME@PROJECT_ID.iam.gserviceaccount.com

# set the credential to the key path
set GOOGLE_APPLICATION_CREDENTIALS=KEY_PATH