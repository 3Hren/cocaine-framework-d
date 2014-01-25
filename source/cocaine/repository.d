module cocaine.repository;

import std.conv;
import std.traits : FunctionTypeOf, ParameterTypeTuple, ReturnType;
import std.typecons;
import std.typetuple;

import cocaine.service;
import cocaine.stream;

private template GetOverloadedMethods(T) {
    import std.typetuple : Filter;

    alias allMembers = TypeTuple!(__traits(allMembers, T));
    template follows(size_t i = 0) {
        static if (i >= allMembers.length) {
            alias follows = TypeTuple!();
        } else static if (!__traits(compiles, mixin("T."~allMembers[i]))) {
            alias follows = follows!(i + 1);
        } else {
            enum name = allMembers[i];

            template isMethod(alias f) {
                static if (is(typeof(&f) F == F*) && is(F == function))
                    enum isMethod = !__traits(isStaticFunction, f);
                else
                    enum isMethod = false;
            }
            alias follows = TypeTuple!(std.typetuple.Filter!(isMethod, __traits(getOverloads, T, name)), follows!(i + 1));
        }
    }
    alias GetOverloadedMethods = follows!();
}

private template staticIota(int beg, int end) {
    static if (beg + 1 >= end) {
        static if (beg >= end) {
            alias TypeTuple!() staticIota;
        } else {
            alias TypeTuple!(+beg) staticIota;
        }
    } else {
        enum mid = beg + (end - beg) / 2;
        alias staticIota = TypeTuple!(staticIota!(beg, mid), staticIota!(mid, end));
    }
}

private template mixinAll(mixins...) {
    static if (mixins.length == 1) {
        static if (is(typeof(mixins[0]) == string)) {
            mixin(mixins[0]);
        } else{
            alias mixins[0] it;
            mixin it;
        }
    } else static if (mixins.length >= 2) {
        mixin mixinAll!(mixins[ 0 .. $/2]);
        mixin mixinAll!(mixins[$/2 .. $ ]);
    }
}

private template hasAttribute(T, Attribute) {
	enum hasAttribute = (staticIndexOf!(Attribute, typeof(__traits(getAttributes, T))) != -1);    
}

private template getServiceName(Service) {
	enum id = staticIndexOf!(CocaineService, typeof(__traits(getAttributes, Service)));
	enum attribute = __traits(getAttributes, Service)[id];
	enum getServiceName = __traits(getMember, attribute, "name");
}

private template FuncInfo(string s, F) {
    enum name = s;
    alias type = F;
}

private template Uniq(Members...) {
    static if (Members.length == 0) {
        alias Uniq = TypeTuple!();
    } else {
        alias Func = Members[0];
        enum name = __traits(identifier, Func);
        alias type = FunctionTypeOf!Func;

        template check(size_t i, mem...) {
            static if (i >= mem.length) {
                enum ptrdiff_t check = -1;
            } else {
                enum ptrdiff_t check = __traits(identifier, Func) == __traits(identifier, mem[i]) &&
                	!is(DerivedFunctionType!(type, FunctionTypeOf!(mem[i])) == void) ? i : check!(i + 1, mem);
            }
        }

        enum ptrdiff_t x = 1 + check!(0, Members[1 .. $]);
        static if (x >= 1) {
            alias typex = DerivedFunctionType!(type, FunctionTypeOf!(Members[x]));
            alias remain = Uniq!(Members[1 .. x], Members[x + 1 .. $]);

            static if (remain.length >= 1 && remain[0].name == name && !is(DerivedFunctionType!(typex, remain[0].type) == void)) {
                alias F = DerivedFunctionType!(typex, remain[0].type);
                alias Uniq = TypeTuple!(FuncInfo!(name, F), remain[1 .. $]);
            } else {
                alias Uniq = TypeTuple!(FuncInfo!(name, typex), remain);
            }
        } else {
            alias Uniq = TypeTuple!(FuncInfo!(name, type), Uniq!(Members[1 .. $]));
        }
    }
}

struct Repository {
	static T create(T)() {
		alias TargetMembers = Uniq!(GetOverloadedMethods!(T));
                    
		final class Implementation : T {
			private Service service;

			private template generateFunction(size_t id) {
				enum name = TargetMembers[id].name;            				
				enum n = to!string(id);
				enum functionBody = "execute!(ReturnType!(TargetMembers["~n~"].type))("~n~", args)";				
				enum generateFunction = 
				"override ReturnType!(TargetMembers["~n~"].type) "~name~"(ParameterTypeTuple!(TargetMembers["~n~"].type) args) "~"{ 
					return "~functionBody~"; 
				}";
			}		

			private T execute(T, Args...)(uint id, Args args) {
				Downstream downstream = service.sendMessage(id, args);
				return downstream.readAll!(T);				
			}

		public:		
			this() {
				static assert(hasAttribute!(T, CocaineService), "cocaine services must be decorated with 'CocaineService' attribute");				 
				string name = getServiceName!T;
				service = new Service(name);
				service.connect();
			}

			mixin mixinAll!(staticMap!(generateFunction, staticIota!(0, TargetMembers.length)));						
		}

		return new Implementation;
	}
}
