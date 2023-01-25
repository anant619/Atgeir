FROM python:3.9
WORKDIR hawkeye
COPY . /hawkeye
RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt
#CMD ["python","test.py"]
#CMD ["python","kpis.py"]
#CMD ["python","Export_KPIs.py"]
ENTRYPOINT ["sh","run.sh"]
