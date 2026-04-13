#### **BSE Stock Data EC2 Login**



ssh -i "D:\\Coding\\Stock Predictor\\Stock-Predictor-v1.1.0\\EC2 Key\\BSE Stock Data\\BSE Stock Data.pem" ubuntu@13.127.144.204



cd /home/ubuntu/BSE\_Stock\_Data

./venv/bin/python auth\_helper.py



sudo systemctl restart bse\_stock\_harvester

sudo journalctl -u bse\_stock\_harvester -f



###### To transfer Data to EC2



aws s3 mv /home/ubuntu/BSE\_Stock\_Data/tick\_data s3://stock-predictor-2026/EC2+Data/BSE+Stock+Data/ --recursive



#### **NSE Stock Option Data EC2 Login**



ssh -i "D:\\Coding\\Stock Predictor\\Stock-Predictor-v1.1.0\\EC2 Key\\NSE Stock Options\\NSE-Stock-Option.pem" ubuntu@15.206.72.170



cd /home/ubuntu/nse\_stock\_option

source venv\_nse/bin/activate



python auth\_helper.py --client api1



python auth\_helper.py --client api2



python nse\_stock\_options\_harvester.py



###### To transfer Data to EC2



aws s3 mv /home/ubuntu/nse\_stock\_option/tick\_data/api1 s3://stock-predictor-2026/EC2+Data/NSE\_Stock\_Data/api1/ --recursive

aws s3 mv /home/ubuntu/nse\_stock\_option/tick\_data/api2 s3://stock-predictor-2026/EC2+Data/NSE\_Stock\_Data/api2/ --recursive



#### **NSE \& BSE Index Options \& Future \& Stock Future Login**



ssh -i "D:\\Coding\\Stock Predictor\\Stock-Predictor-v1.1.0\\EC2 Key\\BSE Index Options \& Future \& Stock Future\\BSE Index Options \& Future \& Stock Future.pem" ubuntu@3.110.62.224



cd /home/ubuntu/NSE\_Index\_Option\_Future\_Stock\_Future

source venv/bin/activate



python auth\_helper.py



python tick\_harvester.py



###### To transfer Data to EC2



aws s3 mv /home/ubuntu/NSE\_Index\_Option\_Future\_Stock\_Future/tick\_data s3://stock-predictor-2026/EC2+Data/NSE\_BSE\_Index\_Options\_Future\_Stock\_Future/ --recursive



#### **NSE Stock Data Login**



ssh -i "D:\\Coding\\Stock Predictor\\Stock-Predictor-v1.1.0\\EC2 Key\\Stock-Prediction-v1.1.0.pem" ubuntu@13.201.136.222



cd /home/ubuntu/NSE\_Stock/Tick\_Data\_Folder

../venv/bin/python auth\_helper.py



sudo systemctl restart tick\_harvester

sudo journalctl -u tick\_harvester -f



###### To transfer Data to EC2



aws s3 mv /home/ubuntu/NSE\_Stock/Tick\_Data\_Folder/tick\_data s3://stock-predictor-2026/EC2+Data/Stock\_Data/ --recursive

