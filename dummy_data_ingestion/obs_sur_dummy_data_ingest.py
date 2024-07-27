import json
import os
import datetime
from kafka import KafkaConsumer, KafkaProducer
from configparser import ConfigParser, ExtendedInterpolation

# Load configuration
config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(os.path.join(config_path[0], "config.ini"))

# Kafka configuration
kafka_url = config.get("KAFKA", "url")
producer = KafkaProducer(bootstrap_servers=[kafka_url])

# Dummy data
slObservation = {
  "roleTitle": "",
  "userBoardName": "",
  "userType": "",
  "organisationId": "",
  "organisationName": "",
  "observationSubmissionId": "",
  "appName": "",
  "solutionType": "",
  "entity": "",
  "entityExternalId": "",
  "course": "",
  "createdBy": "",
  "isAPrivateProgram": "",
  "programExternalId": "",
  "programId": "",
  "programName": "",
  "programDescription": "",
  "solutionExternalId": "",
  "solutionId": "",
  "observationId": "",
  "criteriaExternalId": "",
  "criteriaName": "",
  "criteriaDescription": "",
  "section": "",
  "solutionName": "",
  "scoringSystem": "",
  "solutionDescription": "",
  "questionSequenceByEcm": "",
  "entityType": "",
  "observationName": "",
  "questionId": "",
  "questionAnswer": "",
  "questionResponseType": "",
  "questionResponseLabel": "",
  "questionExternalId": "",
  "questionName": "",
  "questionECM": "",
  "criteriaId": "",
  "completedDate": "",
  "createdAt": "",
  "updatedAt": "",
  "remarks": "",
  "totalEvidences": "",
  "instanceParentQuestion": "",
  "instanceParentId": "",
  "instanceParentResponsetype": "",
  "instanceId": "",
  "instanceParentExternalId": "",
  "instanceParentEcmSequence": "",
  "channel": "",
  "parent_channel": "",
  "submissionNumber": "",
  "submissionTitle": "",
  "criteriaLevelReport": "",
  "isRubricDriven": "",
  "userProfile": ""
}
slObservationMeta = {
    "completedDate": "",
    "createdBy": "",
    "entity": "",
    "entityExternalId": "",
    "entityType": "",
    "isAPrivateProgram": "",
    "observationName": "",
    "observationSubmissionId": "",
    "observationId": "",
    "organisationName": "",
    "solutionType": "",
    "solutionExternalId": "",
    "solutionId": "",
    "solutionName": "",
    "userType": "",
    "userProfile": "",
    "createdAt": ""
}

slObservationStatusStarted = {
    "startedAt": "",
    "observationSubmissionId": ""
}

slObservationStatusInprogress = {
    "inprogressAt": "",
    "observationSubmissionId": ""
}

slObservationStatusCompleted = {
    "completedAt": "",
    "observationSubmissionId": ""
}

slSurvey = {
    "completedDate": "",
    "createdAt": "",
    "createdBy": "",
    "criteriaExternalId": "",
    "criteriaId": "",
    "criteriaName": "",
    "evidence_count": "",
    "evidences": "",
    "isAPrivateProgram": "",
    "organisation_id": "",
    "organisation_name": "",
    "questionAnswer": "",
    "questionECM": "",
    "questionExternalId": "",
    "questionId": "",
    "questionName": "",
    "questionResponseLabel": "",
    "questionResponseLabel_number": "",
    "questionResponseType": "",
    "remarks": "",
    "solutionExternalId": "",
    "solutionId": "",
    "solutionName": "",
    "surveyId": "",
    "surveyName": "",
    "surveySubmissionId": "",
    "total_evidences": "",
    "updatedAt": "",
    "user_type": ""
}

slSurveyMeta = {
    "completedDate": "",
    "createdBy": "",
    "createdAt": "",
    "isAPrivateProgram": "",
    "organisationName": "",
    "solutionExternalId": "",
    "solutionId": "",
    "solutionName": "",
    "surveyName": "",
    "surveySubmissionId": "",
    "surveyId": "",
    "userProfile": ""
}

slSurveyStatusStarted = {
    "startedAt": "",
    "surveySubmissionId": ""
}

slSurveyStatusInprogress = {
    "inprogressAt": "",
    "surveySubmissionId": ""
}

slSurveyStatusCompleted = {
    "completedAt": "",
    "surveySubmissionId": ""
}

def send_to_kafka(topic, data):
    producer.send(topic, json.dumps(data).encode('utf-8'))
    producer.flush()
    print(f"Sent data to {topic}: {data}")

# Sending survey data
send_to_kafka(config.get("KAFKA", "survey_druid_topic"), slSurvey)
send_to_kafka(config.get("KAFKA", "survey_meta_druid_topic"), slSurveyMeta)
send_to_kafka(config.get("KAFKA", "survey_started_druid_topic"), slSurveyStatusStarted)
send_to_kafka(config.get("KAFKA", "survey_inprogress_druid_topic"), slSurveyStatusInprogress)
send_to_kafka(config.get("KAFKA", "survey_completed_druid_topic"), slSurveyStatusCompleted)

# Sending observation data
send_to_kafka(config.get("KAFKA", "observation_druid_topic"), slObservation)
send_to_kafka(config.get("KAFKA", "observation_meta_druid_topic"), slObservationMeta)
send_to_kafka(config.get("KAFKA", "observation_started_druid_topic"), slObservationStatusStarted)
send_to_kafka(config.get("KAFKA", "observation_inprogress_druid_topic"), slObservationStatusInprogress)
send_to_kafka(config.get("KAFKA", "observation_completed_druid_topic"), slObservationStatusCompleted)
