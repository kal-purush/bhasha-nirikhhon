module trans/types

imports

	src-gen/signatures/Alloy-sig

type rules // references

	Ref(e) : ty
	where definition of e : ty

relations // subtype

	define transitive <sub:

type rules // subtype

	SigDecl(a, [s], Some(Extends(super)), c, d) :-
  where super : super-ty
    and store s <sub: super-ty