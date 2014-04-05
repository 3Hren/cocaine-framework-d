module cocaine.detail.util;

import msgpack;

template isMsgpackable(Args...) {
    template helper(size_t i, T...) {
        static if (i >= T.length) {
            enum helper = true;
        } else {
            enum helper = __traits(compiles, msgpack.pack(T[i].init)) && helper!(i + 1, T);
        }
    }

    enum isMsgpackable = helper!(0, Args);
}
