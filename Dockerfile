FROM python:3

ADD main.py .

RUN pip install typer

CMD ["python"]
