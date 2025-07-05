FROM public.ecr.aws/lambda/python:3.9

# Copy your function code
COPY main.py ${LAMBDA_TASK_ROOT}

# Copy any additional files/libraries
COPY requirements.txt .
RUN pip install -r requirements.txt

# Set the CMD to your handler
CMD ["app.lambda_handler"]