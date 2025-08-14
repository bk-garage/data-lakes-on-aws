#!/bin/bash
pflag=false
tflag=false
dflag=false
rflag=false

DIRNAME=$(dirname "$0")

usage () { echo "
    -h -- Opens up this help message
    -p -- Name of the AWS profile to use
    -t -- Team name (required)
    -d -- Dataset name (required)
    -r -- AWS region (required)
"; }
options=':p:t:d:r:h'
while getopts "$options" option
do
    case "$option" in
        p  ) pflag=true; PROFILE=$OPTARG;;
        t  ) tflag=true; TEAM=$OPTARG;;
        d  ) dflag=true; DATASET=$OPTARG;;
        r  ) rflag=true; REGION=$OPTARG;;
        h  ) usage; exit;;
        \? ) echo "Unknown option: -$OPTARG" >&2; exit 1;;
        :  ) echo "Missing option argument for -$OPTARG" >&2; exit 1;;
        *  ) echo "Unimplemented option: -$OPTARG" >&2; exit 1;;
    esac
done

if ! "$tflag"
then
    echo "Team name is required. Use -t <team-name>" >&2
    usage
    exit 1
fi

if ! "$dflag"
then
    echo "Dataset name is required. Use -d <dataset-name>" >&2
    usage
    exit 1
fi

if ! "$rflag"
then
    echo "AWS region is required. Use -r <region>" >&2
    usage
    exit 1
fi

if "$pflag"
then
    echo "using AWS profile $PROFILE..." >&2
fi

echo "using team: $TEAM" >&2
echo "using dataset: $DATASET" >&2
echo "using region: $REGION" >&2

ARTIFACTS_BUCKET=$(aws --region "$REGION" ssm get-parameter --name "/SDLF2/S3/ArtifactsBucket" --query "Parameter.Value" --output text ${PROFILE:+--profile "$PROFILE"})
aws s3 cp "$DIRNAME/scripts/legislators-glue-job.py" "s3://$ARTIFACTS_BUCKET/artifacts/" ${PROFILE:+--profile "$PROFILE"}

mkdir -p "$DIRNAME"/output

function send_data() 
{
  ORIGIN="$DIRNAME/data/"
  
  RAW_BUCKET=$(aws --region "$REGION" ssm get-parameter --name "/SDLF2/S3/RawBucket" --query "Parameter.Value" --output text ${PROFILE:+--profile "$PROFILE"})
  KMS_KEY=$(aws --region "$REGION" ssm get-parameter --name "/SDLF/KMS/$TEAM/DataKeyId" --query "Parameter.Value" --output text ${PROFILE:+--profile "$PROFILE"})

  S3_DESTINATION=s3://$RAW_BUCKET/
  COUNT=0
  for FILE in "$ORIGIN"/*.json;
  do
    (( COUNT++ )) || true
    aws s3 cp "$FILE" "${S3_DESTINATION}${TEAM}/${DATASET}/" --sse aws:kms --sse-kms-key-id "$KMS_KEY" ${PROFILE:+--profile "$PROFILE"}
    echo "transferred $COUNT files"
  done
}

VPC_SUPPORT=$(aws --region "$REGION" ssm get-parameter --name "/SDLF/VPC/Enabled" --query "Parameter.Value" --output text ${PROFILE:+--profile "$PROFILE"} 2>/dev/null)
if [ -z "$VPC_SUPPORT" ]
then
  aws --region "$REGION" ssm put-parameter --name "/SDLF/VPC/Enabled" --value "false" --type String ${PROFILE:+--profile "$PROFILE"}
fi

aws cloudformation package --template-file "$DIRNAME"/scripts/legislators-glue-job.yaml \
  --s3-bucket "$ARTIFACTS_BUCKET" \
  ${PROFILE:+--profile "$PROFILE"} \
  --output-template-file "$DIRNAME"/output/packaged-template.yaml

STACK_NAME="sdlf-${TEAM}-${DATASET}-glue-job"
aws cloudformation deploy \
    --s3-bucket "$ARTIFACTS_BUCKET" --s3-prefix sdlf-utils \
    --stack-name "$STACK_NAME" \
    --template-file "$DIRNAME"/output/packaged-template.yaml \
    --parameter-overrides pTeamName="$TEAM" pDatasetName="$DATASET" \
    --tags Framework=sdlf Team="$TEAM" Dataset="$DATASET" \
    --capabilities "CAPABILITY_NAMED_IAM" "CAPABILITY_AUTO_EXPAND" \
    --region "$REGION" \
    ${PROFILE:+--profile "$PROFILE"} || exit 1

send_data
