FROM python:3.10-slim

# 1. Install Java (Using default-jre ensures compatibility with the Debian version)
# We also install procps (for ps command) and curl
RUN apt-get update && \
    apt-get install -y default-jre curl unzip procps && \
    apt-get clean

# 2. Install Allure CLI
RUN curl -o allure.tgz -L https://github.com/allure-framework/allure2/releases/download/2.24.0/allure-2.24.0.tgz && \
    tar -zxvf allure.tgz -C /opt/ && \
    ln -s /opt/allure-2.24.0/bin/allure /usr/bin/allure && \
    rm allure.tgz

# 3. Set JAVA_HOME to the generic link (Works on M1/Arm64 and Intel/AMD64)
ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app

# 4. Install Python Dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["bash"]

