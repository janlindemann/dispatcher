#pragma once
#include <string>

class Settings {
 public:
  static Settings* get_instance() {
    static Settings* instance = NULL;
    if (instance == NULL) {
      instance = new Settings();
    }
    return instance;
  };
  std::string** hosts;
  int active_hosts_num = 1;
  int failoverdone = 0;
  int number_of_hosts = 1;
  int current_master = 0;
  int* active_hosts;

 private:
  Settings() {};
};
