#include <fstream>
#include <chrono>
#include <thread>
#include <librdkafka/rdkafkacpp.h>

#define TARGET_BPS 625000
#define HEADER_SIZE 24

int main() {
    std::ifstream f("data.ch10", std::ios::binary);
    std::string errstr;

    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", "localhost:9092", errstr);
    RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);

    long sent = 0;
    auto start = std::chrono::steady_clock::now();

    while (f) {
        char sync[2];
        f.read(sync, 2);
        if (!f || sync[0] != 0x25 || sync[1] != (char)0xEB) continue;

        char header[HEADER_SIZE];
        header[0] = sync[0];
        header[1] = sync[1];
        f.read(header + 2, HEADER_SIZE - 2);

        uint32_t plen = *(uint32_t*)(header + 4);
        std::string packet(header, HEADER_SIZE);
        packet.resize(plen);
        f.read(&packet[HEADER_SIZE], plen - HEADER_SIZE);

        producer->produce(
            "ch10.raw.frames",
            RdKafka::Topic::PARTITION_UA,
            RdKafka::Producer::RK_MSG_COPY,
            packet.data(), packet.size(),
            nullptr, 0, 0, nullptr
        );

        sent += packet.size();
        auto now = std::chrono::steady_clock::now();
        double elapsed = std::chrono::duration<double>(now - start).count();
        if (elapsed > 0 && sent / elapsed > TARGET_BPS)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    producer->flush(5000);
    delete producer;
    delete conf;
}
