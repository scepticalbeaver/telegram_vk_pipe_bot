#include <algorithm>
#include <iostream>
#include <fstream>
#include <string>
#include <thread>
#include <chrono>

using namespace std;

string ReplaceAll(std::string str, const std::string& from, const std::string& to) {
    size_t start_pos = 0;
    while((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length(); // Handles case where 'to' is a substring of 'from'
    }
    return str;
}


int main(int argc, char* argv[])
{
    if (argc < 2 || argv[0] == "--help") {
        cout << "First argument must be a path to a file with credentials" <<endl;
    }

    string d = argv[0];
    string s = argv[1];

    this_thread::sleep_for(std::chrono::seconds(1));

    fstream file;
    file.open(s, ios_base::in);

    if (!file.is_open()) {
        cerr << "Bad file path or no permissions" << endl;
        exit(EXIT_FAILURE);
    }

    file >> d;
    file >> d;
    file >> s;

    reverse(d.begin(), d.end());
    s = ReplaceAll(s, "vv", "vvvv");
    reverse(s.begin(), s.end());

    s = ReplaceAll(s, "!", "");
    d = ReplaceAll(d, "!", "");
    s = ReplaceAll(s, "Z", "");
    s = ReplaceAll(s, "X", "ni");
    d = ReplaceAll(d, "z", "");
    s = ReplaceAll(s, "z", "");
    d = ReplaceAll(d, "X", "ni");
    d = ReplaceAll(d, "Z", "");

    reverse(s.begin(), s.end());
    reverse(d.begin(), d.end());

    s = ReplaceAll(s, "8", "s");
    d = ReplaceAll(d, "v", "1");
    s = ReplaceAll(s, "v", "1");


    int payload_size = s.size();
    s = s + d;

    for (int i = 0; i < payload_size; i++)
        cout << s[i];
    cout << endl;

    file.close();

    return 0;
}

