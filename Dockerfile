FROM theosotr/sqlite3-test

RUN sudo apt update && \
    sudo apt install -y python3 python3-pip && \
    sudo apt install -y python3 python3-pip unzip && \
    pip3 install tqdm

WORKDIR /app

COPY source_code.zip .
COPY reducer .

USER root
RUN unzip source_code.zip && rm source_code.zip && \
    find /app/queries -name "*.sh" -exec chmod +x {} \;
RUN sed -i 's/\r//' /app/reducer
RUN chmod +x /app/reducer && mv /app/reducer /usr/bin/reducer

CMD ["bash"]

