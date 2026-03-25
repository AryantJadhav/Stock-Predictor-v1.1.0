pipeline {
    agent any

    environment {
        BUCKET = "stock-predictor-2026"
        BASE_DIR = "/home/ubuntu/Stock-Predictor/Tick_Data_Folder/tick_data"
    }

    stages {

        stage('Upload fo_data') {
            steps {
                sh '''
                echo "Uploading fo_data..."
                aws s3 sync $BASE_DIR/fo_data s3://$BUCKET/tick_data/fo_data
                '''
            }
        }

        stage('Upload stock_price_data') {
            steps {
                sh '''
                echo "Uploading stock_price_data..."
                aws s3 sync $BASE_DIR/stock_price_data s3://$BUCKET/tick_data/stock_price_data
                '''
            }
        }

        stage('Verify Upload') {
            steps {
                sh '''
                echo "Verifying uploads..."
                aws s3 ls s3://$BUCKET/tick_data/fo_data
                aws s3 ls s3://$BUCKET/tick_data/stock_price_data
                '''
            }
        }

        stage('Delete Local Data') {
            steps {
                sh '''
                echo "Deleting local files..."
                rm -rf $BASE_DIR/fo_data/*
                rm -rf $BASE_DIR/stock_price_data/*
                '''
            }
        }
    }

    post {
        success {
            echo 'All data uploaded and cleaned successfully!'
        }
        failure {
            echo 'Upload failed — files NOT deleted.'
        }
    }
}