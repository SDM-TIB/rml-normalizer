# Use an official Python runtime as a parent image
FROM python:3.7

# Set the working directory to /app
WORKDIR /

# Copy the current directory contents into the container at /app
ADD . /app

# Install any needed packages specified in requirements.txt
RUN cd /app && pip3 install --trusted-host pypi.python.org -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 4000

# Define environment variable
#ENV NAME RDFizer

#RUN apk add --no-cache git maven
RUN apt-get update -y && apt-get install maven -y

RUN apt-get update && apt-get install -y curl
RUN curl -sL https://deb.nodesource.com/setup_12.x | bash -
RUN apt-get update && apt-get install -y nodejs


RUN set -e; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        software-properties-common \
    ; \
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 0xB1998361219BD9C9; \
    apt-add-repository 'deb http://repos.azulsystems.com/debian stable main'; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        zulu-8 \
    ; \
    apt-get clean; \
    rm -rf /var/tmp/* /tmp/* /var/lib/apt/lists/*

RUN set -e; \
    pip install JPype1

# Run app.py when the container launches
#CMD ["python3", "/app/data-generator/source/run_generator.py", "3",".//app//source//data//"]

#CMD ["python3", "/app/source/run_normalization.py", ".//app//source//configfile.ini//",".//app//experiments//csv//",".//app//experiments//mappings//"]

WORKDIR /app/

#CMD ["python3", "run_expriments.py"]
CMD ["python3", "run_all.py"]