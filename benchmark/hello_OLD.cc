#include <memory>
#include <remus/remus.h>

int main(int argc, char **argv) {
  remus::INIT();

  // Configure and parse the arguments
  auto args = std::make_shared<remus::ArgMap>();
  args->import(remus::ARGS);
  args->parse(argc, argv);
  if (args->bget(remus::HELP)) {
    args->usage();
    return 0;
  }
  args->report_config();
}
