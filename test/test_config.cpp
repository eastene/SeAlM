//
// Created by evan on 5/9/19.
//

#include <catch2/catch.hpp>

#include "../lib/config.hpp"

TEST_CASE("configuration file read correctly" "[ConfigParser]") {
    std::experimental::filesystem::path path ("test.ini");

    std::ofstream fout(path);
    bool loaded = true;
    try {
        ConfigParser cfp(path);
    } catch (ConfigNotFoundException &cnfe) {
        loaded = false;
    }

    REQUIRE(loaded);

    fout << "teST_key=test_vAl\n";
    fout << "  ws_key=  ws_val   \n";
    fout << "int_key  =1202\n";
    fout << "dble_key=1202.213\n";
    fout << "dbl_ws_key=1202.   213\n";
    fout.close();


    ConfigParser cfp(path);
    REQUIRE(cfp.get_val("test_key") == "test_val");
    REQUIRE(cfp.get_val("ws_key") == "ws_val");
    REQUIRE(cfp.get_long_val("int_key") == 1202);
    REQUIRE(cfp.get_double_val("dble_key") == 1202.213);
    REQUIRE(cfp.get_double_val("dbl_ws_key") == 1202.213);


    std::remove("test.ini");
}