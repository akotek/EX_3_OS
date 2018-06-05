// ------------------------------ includes ------------------------------
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include <stdio.h>
#include <signal.h>
#include <sys/time.h>
#include <setjmp.h>
#include <unistd.h>
#include "string.h"
#include <unistd.h>
#include <queue>
#include <iostream>
#include <dirent.h>
#include <sys/stat.h>
#include <iostream>
#include <fstream>
//#include "MapReduce"
// -------------------------- using definitions -------------------------
using namespace std;
// -------------------------- definitions -------------------------------
//typedef std::list<V2 *> V2_LIST;

class FileToSearch : public K1 {
public:
	string file;
	FileToSearch(string file2):file(file2){};
	bool operator<(const K1 &other) const{return true;};
	~FileToSearch(){};
};

class WordToSearch : public V1 {
public:
	WordToSearch(string word2): word(word2){};
	string word;
};

class Word : public K2 {
public:
	string word;
	Word(string word2):word(word2){};
	bool operator<(const K2 &other) const{
		Word* temp = (Word*) &other;
		Word* temp2 = (Word*) this;
		if(temp -> word < temp2 -> word){
			return true;
		}
		return false;
	}
	~Word(){};
};

class ApperanceOfWord : public V2 {
public:
	ApperanceOfWord(int num2): num(num2){};
	int num;
	~ApperanceOfWord(){};

};


class Word2 : public K3 {
public:
	string word2;
	Word2(string word3): word2(word3){};
	bool operator<(const K3 &other) const{
		Word2* temp = (Word2*) &other;
		Word2* temp2 = (Word2*) this;
		if(temp2 -> word2 < temp -> word2){
			return true;
		}
		return false;
	}
	~Word2(){};
};

class ApperanceOfWordList : public V3{
public:
	ApperanceOfWordList(int num2): num(num2){};
	int num;
	~ApperanceOfWordList(){};

};

vector<K2*> k2BaseVec;

class Count : public MapReduceClient{
public:

	void map(const K1 *const key, const V1 *const val, void* context) const{

		DIR *dir;
		struct dirent *ent;
		string file = ((FileToSearch*) key) -> file;
		ifstream myfile;
		myfile.open(file);
		string wordToSearch = ((WordToSearch*) val) -> word;
        while (myfile.good())
        {
            string word;
            myfile >> word;

            if (word == wordToSearch)
            {
	            ApperanceOfWord* apperanceOfWord = new ApperanceOfWord(1);
	        	Word* k2 = new Word(wordToSearch);
//	        	 k2BaseVec.push_back(k2);
				emit2(k2, apperanceOfWord, context);
        	}
        }
        myfile.close();	
        delete key;
        delete val;
	}

    void reduce(const IntermediateVec* pairs, void* context) const{

		string word = ((Word*)pairs->at(0).first) -> word;
		int count = 0;
		for (const IntermediatePair& pair: *pairs)
		{
			count += ((ApperanceOfWord*)pair.second) -> num;
			delete pair.first;
			delete pair.second;
		}

		Word2* myWord2 = new Word2(word);

		ApperanceOfWordList* apperanceOfWordList = new ApperanceOfWordList(count);
		emit3(myWord2,apperanceOfWordList, context);


    }
};


InputVec getData(int argc,char *argv[])
{
	InputVec res;

	for (int i = 5; i < 10; ++i)
	{
		for (int j = 1; j < 5; ++j)
		{
			FileToSearch* file1 = new FileToSearch(argv[i]);
			WordToSearch* word1 = new WordToSearch(argv[j]);
			res.push_back(InputPair(file1,word1));
		}
	}
	return res;
}

void compare(OutputVec& temp,OutputVec& finalRes)
{
	auto a = temp.begin();
	auto b = finalRes.begin();
	int size = temp.size();
	int size2 = finalRes.size();
	if (size != size2)
	{
		cerr << "diffrence in output size depend on thread number" << endl;
		cout << size << " " << size2 << endl;
		cout << "exiting test" << endl;
		exit(1);
	}
	for (int i = 0; i < size; ++i)
	{
		Word2 *tempAFirst = (Word2 *) (*a).first;
		ApperanceOfWordList *tempASecond = (ApperanceOfWordList *) (*a).second;

		Word2 *tempBFirst = (Word2 *) (*b).first;
		ApperanceOfWordList *tempBSecond = (ApperanceOfWordList *) (*b).second;

//		if (tempAFirst -> word2 != tempBFirst -> word2)
//		{

//			cerr << "diffrence in content of k3base output depend on thread number" << endl;
		cout << tempAFirst->word2 << " " << tempBFirst->word2 << endl;
		cout << tempASecond->num << " " << tempBSecond->num << endl;
//			cout << "exiting test" << endl;
//			exit(1);
//		}
//		if ( tempASecond -> num != tempBSecond -> num)
//		{
//			cerr << "diffrence in content of k3val output depend on thread number" << endl;
		cout << tempAFirst->word2 << " " << tempBFirst->word2 << endl;
		cout << tempASecond->num << " " << tempBSecond->num << endl;
//			cout << "exiting test" << endl;
//			exit(1);
//		}
		a++;
		b++;
//	}
	}
}

int main(int argc,char *argv[])
{

	struct timeval diff, startTV, endTV;
	Count count;
	InputVec data;
//	FileToSearch F1("/cs/usr/yanivswisa/safe/OS-2018-ex3/test/1Dog1Cat");
//	WordToSearch W1("cat");
//	data.push_back({&F1, &W1});

	OutputVec finalRes;
	OutputVec compareTo;

	data = getData(argc,argv);
	runMapReduceFramework(count,data,compareTo,3);
	finalRes.clear();
	data.clear();
//
	for (int i = 1; i < 10; ++i)
	{
		data = getData(argc,argv);

		runMapReduceFramework(count, data,finalRes,i);

		compare(compareTo, finalRes);


		for(OutputPair temp : finalRes)
		{
			delete temp.first;
			delete temp.second;
		}
		finalRes.clear();
		data.clear();
	}
//
	data = getData(argc,argv);
	runMapReduceFramework(count,data,finalRes,12);



	cout << "***************************************" << endl;
	cout << "uncle sam has a farm,but all his animals escaped and hid" << endl;
	cout << "kids,help uncle sam find his animals in the files" << endl;
	cout << "***************************************" << endl;
	cout << "Recieved: " << endl;

	for(OutputPair temp : finalRes)
	{

		Word2* temp1 = (Word2*) temp.first;
		ApperanceOfWordList* temp2 = (ApperanceOfWordList*) temp.second;
		cout << temp2 -> num << " "<< temp1 -> word2 << endl;
	}

	cout << "***************************************" << endl;
	cout << "Excpected: " << endl;
	cout << "(the order matter,mind you)" << endl;
	cout << "6 Cat " << endl;
	cout << "12 Dog " << endl;
	cout << "10 Fish " << endl;
	cout << "8 Sheep " << endl;

	for(OutputPair temp : finalRes)
	{
		delete temp.first;
		delete temp.second;
	}
	for(OutputPair temp : compareTo)
	{
		delete temp.first;
		delete temp.second;
	}
//	 cout << "start delete k2" << endl;
//	 for(k2Base* k2 : k2BaseVec)
//	 {
////
//	 	Word* k2word = (Word*) k2;
//	 	cout << k2word -> word << " " << k2  << endl;
//	 	delete k2;
//	 }
//	 cout << "finish delete k2" << endl;
	return 0;
}