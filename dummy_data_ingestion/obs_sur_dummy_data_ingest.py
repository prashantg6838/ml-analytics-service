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
slObservation = {'roleTitle': '', 'userBoardName': None, 'userType': '', 'organisationId': '01392275379456409617', 'organisationName': '', 'observationSubmissionId': '60111ac82d0bbd2f0c3229ce', 'appName': '', 'solutionType': 'observation_with_out_rubric', 'entity': 'do_1234567', 'entityExternalId': '', 'course': 'do_1234567', 'createdBy': '828a16a4-a20f-4ed5-b8f5-7a96770a31c1', 'isAPrivateProgram': False, 'programExternalId': 'Testing Observation for Support', 'programId': '660150444692d300086005c6', 'programName': 'Testing Observation for Support', 'programDescription': 'Program', 'solutionExternalId': '82fb3fe4-ea91-11ee-9c5f-d5a85c1801d4-OBSERVATION-TEMPLATE_CHILD', 'solutionId': '6601504bd26a000008496966', 'observationId': '660253ec076ebd0008b14fdd', 'criteriaExternalId': 'C1_1711362113845-1711362123655', 'criteriaName': 'Criteria 1', 'criteriaDescription': 'Criteria 1', 'section': '', 'solutionName': 'Testing Observation for Support', 'scoringSystem': 'None', 'solutionDescription': 'Observation Solution', 'questionSequenceByEcm': None, 'entityType': 'course', 'observationName': 'Testing Observation for Support', 'questionId': '65fd59e9d2bc56000784c467', 'questionAnswer': 'gag', 'questionResponseType': 'text', 'questionResponseLabel': "'gag'", 'questionExternalId': 'SUR_TEST_001_1710387379109-1710387389092', 'questionName': 'What are the challenges that you are facing in enrolment?', 'questionECM': 'OB', 'criteriaId': '65fd59e9d2bc56000784c478', 'completedDate': '2024-03-26T16:14:10.690Z', 'createdAt': '2024-03-26T16:12:49.079Z', 'updatedAt': '2024-03-26T16:14:10.691Z', 'remarks': '', 'totalEvidences': 0, 'instanceParentQuestion': '', 'instanceParentId': '', 'instanceParentResponsetype': '', 'instanceId': '', 'instanceParentExternalId': '', 'instanceParentEcmSequence': '', 'channel': '01392275379456409617', 'parent_channel': 'SHIKSHALOKAM', 'submissionNumber': 1, 'submissionTitle': 'Observation 1', 'criteriaLevelReport': 'False', 'isRubricDriven': False, 'userProfile': ''}

slObservationMeta = {
    "completedDate": "2024-03-22T05:55:59.040Z",
    "createdBy": "6778cc2b-8075-49a4-9e7d-deafa4d0d9ef",
    "entity": "567",
    "entityExternalId": "",
    "entityType": "course",
    "isAPrivateProgram": "false",
    "observationName": "dummy_test",
    "observationSubmissionId": "65fd1d6fd2bc56000784c18d",
    "observationId": "65e82048fa030d00078647df",
    "organisationName": "",
    "solutionType": "observation_with_out_rubric",
    "solutionExternalId": "b048d95a-d490-11ee-a31f-a9298aa1a2bb-OBSERVATION-dummy",
    "solutionId": "65dc65f9a9d408000795b4c8",
    "solutionName": "dummy_test",
    "userType": "",
    "userProfile": "",
    "createdAt": "2024-03-22T05:55:59.040Z"
}

slObservationStatusStarted = {
    "startedAt": "2024-04-22T13:37:28.289Z",
    "observationSubmissionId": "66264eabf805f30008243712"
}

slObservationStatusInprogress = {
    "inprogressAt": "2024-04-22T13:37:28.289Z",
    "observationSubmissionId": "66264eabf805f30008243712"
}

slObservationStatusCompleted = {
    "completedAt": "2024-04-22T13:37:28.289Z",
    "observationSubmissionId": "66264eabf805f30008243712"
}

slSurvey = {
    "completedDate": "2024-07-18T06:58:19.373Z",
    "createdAt": "2024-07-18T06:57:17.862Z",
    "createdBy": "de5373f4-d74b-4210-80c3-0d7ec283e300",
    "criteriaExternalId": "8d796bba-0c34-11ef-953a-c0a5e8ff50d1-1715060533757-SF",
    "criteriaId": "6639bf35b0610e0008d23728",
    "criteriaName": "Survey and Feedback",
    "evidence_count": "",
    "evidences": "",
    "isAPrivateProgram": "false",
    "organisation_id": "0126796199493140480",
    "organisation_name": "Staging Custodian Organization",
    "questionAnswer": "gyh",
    "questionECM": "SF",
    "questionExternalId": "SUR_TEST_001_1715060530351-1715060533762",
    "questionId": "6639bf35b0610e0008d23717",
    "questionName": "Enter your First question",
    "questionResponseLabel": "'gyh'",
    "questionResponseLabel_number": 0,
    "questionResponseType": "text",
    "remarks": "",
    "solutionExternalId": "8d796bba-0c34-11ef-953a-c0a5e8ff50d1-1715060533757",
    "solutionId": "6639bf35b0610e0008d2372c",
    "solutionName": "testing survey for start date and end date",
    "surveyId": "6698bccde504bc0008bc06c2",
    "surveyName": "testing survey for start date and end date",
    "surveySubmissionId": "6698bccde504bc0008bc06cb",
    "total_evidences": "0",
    "updatedAt": "2024-07-18T06:58:19.373Z",
    "user_type": "administrator"
}

slSurveyMeta = {
    "completedDate": "2024-04-22T13:37:28.289Z",
    "createdBy": "539de87f-8f72-479a-be03-a87cb4e2d7fc",
    "createdAt": "2024-04-22T11:48:59.585Z",
    "isAPrivateProgram": "false",
    "organisationName": "",
    "solutionExternalId": "dd73c82e-008a-11ef-a7a3-ac12038f3f22-1713778193064",
    "solutionId": "66262e11f805f300082438ff",
    "solutionName": "dummy-event",
    "surveyName": "dummy-event",
    "surveySubmissionId": "66264eabf805f30008243712",
    "surveyId": "66264eabf805f3000824396f",
    "userProfile": ""
}

slSurveyStatusStarted = {
    "startedAt": "2024-04-22T13:37:28.289Z",
    "surveySubmissionId": "66264eabf805f30008243712"
}

slSurveyStatusInprogress = {
    "inprogressAt": "2024-04-22T13:37:28.289Z",
    "surveySubmissionId": "66264eabf805f30008243712"
}

slSurveyStatusCompleted = {
    "completedAt": "2024-04-22T13:37:28.289Z",
    "surveySubmissionId": "66264eabf805f30008243712"
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
