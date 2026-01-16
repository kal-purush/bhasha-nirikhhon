
# Format	C Type	Python type	Standard size	Notes
# x	pad byte	no value
# c	char	string of length 1	1
# b	signed char	integer	1	(3)
# B	unsigned char	integer	1	(3)
# ?	_Bool	bool	1	(1)
# h	short	integer	2	(3)
# H	unsigned short	integer	2	(3)
# i	int	integer	4	(3)
# I	unsigned int	integer	4	(3)
# l	long	integer	4	(3)
# L	unsigned long	integer	4	(3)
# q	long long	integer	8	(2), (3)
# Q	unsigned long long	integer	8	(2), (3)
# f	float	float	4	(4)
# d	double	float	8	(4)
# s	char[]	string
# p	char[]	string
# P	void *	integer	 	(5), (3)



from struct import *




def thread_info_process(f):
    f_name = f[0]
    a = f[1]
    # print(z)
    print("thread_info: 0x"+f_name[4:])
    print("task_struct: "+hex(unpack("<I", a[0:4])[0]))
    print("exec_domain: "+hex(unpack("<I", a[4:8])[0]))
    print("flags: "+hex(unpack("<I", a[8:12])[0]))
    print("status: "+hex(unpack("<I", a[12:16])[0]))
    print("cpu: "+hex(unpack("<I", a[16:20])[0]))
    print("preempt_count: "+hex(unpack("<L",a[20:24])[0]))
    print("addr_limit: "+hex(unpack("<L",a[24:28])[0]))


files = ["file1ab1000" ,  "file376c2000",
"file3752d000" , "file37716000",
"file375af000"  ,"file3772f000",
"file375c8000",  "file1ab1000"]
parse = '>'+('c'*(8192))
# a = open("file1ab1000","rb").read()
read_files = [(f,open(f, "rb").read()) for f in files]

for i in read_files:
    thread_info_process(i)
    print("-------------------------")

# struct task_info {
#  int task_offset;			/**< Offset of task_struct in the thread_info struct. */
# #ifdef CPU_DEFS_H
#  /* included from qemu code */
#  /** The address of the `task_struct` of `init`. Can be used to traverse the `task_struct` list. */
#  target_ulong init_addr;
# #else
#  /* used to compile the kernelinfo module */
#  /** The address of the `task_struct` of `init`. Can be used to traverse the `task_struct` list. */
#  void *init_addr;
# #endif
#  size_t size;				/**< Size of `struct task_struct`. */
#  int tasks_offset;			/**< TODO: add documentation for the rest of the struct members */
#  int pid_offset;
#  int tgid_offset;
#  int group_leader_offset;
#  int thread_group_offset;
#  int real_parent_offset;
#  int parent_offset;
#  int mm_offset;
#  int stack_offset;
#  int real_cred_offset;
#  int cred_offset;
#  int comm_offset;			/**< Offset of the command name in `struct task_struct`. */
#  size_t comm_size;			/**< Size of the command name. */
#  int files_offset;			/**< Offset for open files information. */
# };
# size_t = "I"
# int_t = "I"
# void_p = "P"
# task_info = int_t+int_t+void_p+size_t+(int_t*12)+size_t+size_t
# cred_into = int_t*4
# mm_info = int_t*6
# vma_info = int_t*6
# fs_info = int_t*12



# print(len(a))
# # 7840
# print(hex(ord(a[7839])))
# print(hex(ord(a[7840])))


# for i in read_files:
    # print(, )


# z = unpack(parse, a[0:len(a)])
# # print(z)
# for i,j in zip(z,range(8192)):
#     print(j,hex(ord(i)))
# for i in z:
#     if i == 560:
#         print(i)