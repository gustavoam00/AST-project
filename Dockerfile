FROM theosotr/sqlite3-test

RUN sudo apt update && \
    sudo apt install -y python3 python3-pip && \
    pip3 install tqdm

WORKDIR /usr/bin/test-db

CMD ["bash"]