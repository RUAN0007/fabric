export CGO_LDFLAGS='-L/home/ruanpingcheng/Desktop/USTORE/build/lib -lprovchain_lib -lboost_system:$CGO_LDFLAGS='
export CGO_CPPFLAGS='-I/home/ruanpingcheng/Desktop/USTORE/include -I/home/ruanpingcheng/Desktop/USTORE/build/include -I/home/ruanpingcheng/Desktop/USTORE/prov_go/prov_chain/include:$CGO_CPPFLAGS'
make peer
