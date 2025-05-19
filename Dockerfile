FROM theosotr/sqlite3-test

RUN sudo apt update && \
    sudo apt install -y python3 python3-pip && \
    pip3 install tqdm

WORKDIR /app

COPY . .

USER root
RUN sed -i 's/\r//' /app/reducer
RUN chmod +x /app/reducer && mv /app/reducer /usr/bin/reducer

CMD ["bash"]

