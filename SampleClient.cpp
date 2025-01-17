//#include "../MapReduceFramework.h"
//#include "../MapReduceClient.h"
#include <cstdio>
#include <string>
#include <array>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"

class VString : public V1 {
public:
	VString(std::string content) : content(content) { }
	std::string content;
};

class KChar : public K2, public K3{
public:
	KChar(char c) : c(c) { }
	virtual bool operator<(const K2 &other) const {
		return c < static_cast<const KChar&>(other).c;
	}
	virtual bool operator<(const K3 &other) const {
		return c < static_cast<const KChar&>(other).c;
	}
	char c;
};

class VCount : public V2, public V3{
public:
	VCount(int count) : count(count) { }
	int count;
};


class CounterClient : public MapReduceClient {
public:
	void map(const K1* key, const V1* value, void* context) const {
		std::array<int, 256> counts;
		counts.fill(0);
		for(const char& c : static_cast<const VString*>(value)->content) {
			counts[(unsigned char) c]++;
		}

		for (int i = 0; i < 256; ++i) {
			if (counts[i] == 0)
				continue;

			KChar* k2 = new KChar(i);
			VCount* v2 = new VCount(counts[i]);
			emit2(k2, v2, context);
		}
	}

	virtual void reduce(const IntermediateVec* pairs,
		void* context) const {
		const char c = static_cast<const KChar*>(pairs->at(0).first)->c;
		int count = 0;
		for(const IntermediatePair& pair: *pairs) {
			count += static_cast<const VCount*>(pair.second)->count;
			delete pair.first;
			delete pair.second;
		}
		KChar* k3 = new KChar(c);
		VCount* v3 = new VCount(count);
		emit3(k3, v3, context);
	}
};


int main(int argc, char** argv)
{
	CounterClient client;
	InputVec inputVec;
	OutputVec outputVec; // starts empty
	for (int i = 0; i < 20000; i++){
		VString s1("A");
		VString s2("BB");
		VString s3("CCC");
    	VString s4("DDDD");
    	VString s5("EEEEE");
    	VString s6("FFFFFFFF");
		inputVec.push_back({nullptr, &s1});
		inputVec.push_back({nullptr, &s2});
		inputVec.push_back({nullptr, &s3});
		inputVec.push_back({nullptr, &s4});
		inputVec.push_back({nullptr, &s5});
		inputVec.push_back({nullptr, &s6});
	}
	runMapReduceFramework(client, inputVec, outputVec, 4);
    // 4

	for (OutputPair& pair: outputVec) {
		char c = ((const KChar*)pair.first)->c;
		int count = ((const VCount*)pair.second)->count;
		printf("The character %c appeared %d time%s\n",
			c, count, count > 1 ? "s" : "");
		delete pair.first;
		delete pair.second;
	}

	return 0;
}

