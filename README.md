# mcf-scraper-cloudformation
This is a AWS cloudformation template file that deploy a Step Function scraper that would invoke a series of lambda function that would scrape a job portal website daily.

## Deployment via AWS CLI
To deploy this cloudformation template yaml file you would need to ensure your user account could create AIM role, create S3 bucket, create lambda function, and create step function.
You can run the following in your terminal to deploy the cloudformation
```bash
aws cloudformation deploy --template-file mcf_scraper.yml --stack-name mcf_scraper
```

## Resources that would be created
Step Function State Machine - MCFScraperSM
Eventbridge                 - MCFDailyTrigger
Lambda Function             - GetMaxSearch
                            - GetRawData
                            - TransformRawData
IAM Role                    - MCFScraperStateMachine
                            - ETLLambdaIAM

## Description


## Cost


