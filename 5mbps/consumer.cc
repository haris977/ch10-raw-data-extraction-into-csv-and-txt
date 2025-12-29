#include <fstream>
#include <iostream>
#include <librdkafka/rdkafkacpp.h>

int main() {
    std::string errstr;
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", "localhost:9092", errstr);
    conf->set("group.id", "c5", errstr);
    conf->set("auto.offset.reset", "earliest", errstr);

    RdKafka::KafkaConsumer *consumer =
        RdKafka::KafkaConsumer::create(conf, errstr);

    consumer->subscribe({"ch10.raw.frames"});

    std::ofstream out("dump_5mbps.txt");
    int idx = 1;

    while (true) {
        RdKafka::Message *msg = consumer->consume(1000);
        if (msg->err() == RdKafka::ERR_NO_ERROR) {
            out << "PACKET " << idx++ << "\n";
            const uint8_t *p = (const uint8_t*)msg->payload();
            for (size_t i = 0; i + 3 < msg->len(); i += 4) {
                char buf[9];
                sprintf(buf, "%02X%02X%02X%02X",
                        p[i], p[i+1], p[i+2], p[i+3]);
                out << buf << " ";
            }
            out << "\n\n";
            out.flush();
        }
        delete msg;
    }
}
