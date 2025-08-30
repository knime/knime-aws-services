#!groovy
def BN = (BRANCH_NAME == 'master' || BRANCH_NAME.startsWith('releases/')) ? BRANCH_NAME : 'releases/2025-12'

library "knime-pipeline@$BN"

properties([
    pipelineTriggers([
        upstream("knime-aws/${env.BRANCH_NAME.replaceAll('/', '%2F')}")
    ]),
    parameters(workflowTests.getConfigurationsAsParameters()),
    buildDiscarder(logRotator(numToKeepStr: '5')),
    disableConcurrentBuilds()
])

MINIO_IMAGE = "minio/minio:RELEASE.2021-11-03T03-36-36Z"

try {
    // provide the name of the update site project
    knimetools.defaultTychoBuild('org.knime.update.aws-services')

    withEnv([ "KNIME_S3_COMPATIBLE_USER=AKIAIOSVODTN7",
              "KNIME_S3_COMPATIBLE_PASSWORD=wJalrXUtnXpiGh/7MDENG/bPxRfiCY" ]) {

        workflowTests.runTests(
            dependencies: [
                repositories: [
                    'knime-credentials-base',
                    'knime-gateway',
                    'knime-aws',
                    'knime-bigdata',
                    'knime-bigdata-externals',
                    'knime-cloud',
                    'knime-credentials-base',
                    'knime-database',
                    'knime-datageneration',
                    'knime-expressions',
                    'knime-gateway',
                    'knime-jep',
                    'knime-js-base',
                    'knime-json',
                    'knime-filehandling',
                    'knime-textprocessing',
                    'knime-database',
                    'knime-kerberos',
                    'knime-streaming', 
                    'knime-office365',
                    'knime-rest',
                    'knime-xml',
                    'knime-aws-services'
                ]
            ],
            sidecarContainers: [
                [ image: MINIO_IMAGE, namePrefix: "S3_COMPATIBLE", cmd: 'server /data', port: 9000,
                    envArgs: [
                        "MINIO_ROOT_USER=${env.KNIME_S3_COMPATIBLE_USER}",
                        "MINIO_ROOT_PASSWORD=${env.KNIME_S3_COMPATIBLE_PASSWORD}"
                    ]
                ]
            ]
        )
    }

    stage('Sonarqube analysis') {
        env.lastStage = env.STAGE_NAME
        workflowTests.runSonar()
    }
} catch (ex) {
    currentBuild.result = 'FAILURE'
    throw ex
} finally {
    notifications.notifyBuild(currentBuild.result);
}

/* vim: set shiftwidth=4 expandtab smarttab: */
