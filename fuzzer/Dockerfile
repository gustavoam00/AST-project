FROM theosotr/sqlite3-test

RUN sudo apt update && \
    sudo apt install -y python3 python3-pip && \
    pip3 install tqdm

WORKDIR /app

COPY . .

USER root
RUN sed -i 's/\r//' /app/test-db
RUN chmod +x /app/test-db && mv /app/test-db /usr/bin/test-db

CMD ["bash"]

