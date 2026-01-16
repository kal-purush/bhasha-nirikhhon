import pickle
import uuid


class Type:
    FILE = "FILE"
    LINK = "LINK"


class Tools:
    @staticmethod
    def generate_id():
        return uuid.uuid4()


class OperationType:
    MOUNT = "MOUNT"
    UNMOUNT = "UNMOUNT"
    FILE_STAT = "FILE_STAT"
    LS = "LS"
    CREATE = "CREATE"
    OPEN = "OPEN"
    CLOSE = "CLOSE"
    WRITE = "WRITE"
    LINK = "LINK"
    UNLINK = "UNLINK"
    TRUNCATE = "TRUNCATE"
    READ = "READ"
    W = "W"


class SystemFile:
    def __init__(self, file_id, file_type, file_name, size, links, file_blocks, uuids):
        self.file_id = file_id
        self.file_type = file_type
        self.file_name = file_name
        self.size = size
        self.links = links
        self.file_blocks = file_blocks
        self.uuids = uuids

    def __str__(self):
        return f"SystemFile {{file_type={self.file_type}, file_name={self.file_name}, size={self.size}}}"


class Api:
    def __init__(self):
        self.gson = None
        self.listType = None

    @staticmethod
    def save_to_file_system(file_system: str):
        with open("file_system.pickle", "wb") as f:
            pickle.dump(file_system, f)

    @staticmethod
    def load_file_system():
        with open("file_system.pickle", "rb") as f:
            return pickle.load(f)


class SystemDriver:
    BYTES_FOR_BLOCK = 30
    file_blocks = []
    api = Api()
    files: [SystemFile] = []

    def w(self):
        if len(self.files) == 0:
            print("File system needs mounting")
        for i in self.files:
            print(i)

    def mount(self):
        self.files: [SystemFile] = self.api.load_file_system()

    def unmount(self):
        print("unmount")
        self.files = []

    def load_file_blocks(self):
        for file in self.files:
            if file.file_blocks is not None:
                for file_block in file.file_blocks:
                    self.file_blocks.append(file_block)

    def next_free_block(self):
        count = 0
        for i in self.file_blocks:
            if i != count:
                return count
            i += 1
        return 0

    def is_name_unique(self, file_name):
        is_unique = True
        for file in self.files:
            if file.file_name == file_name:
                is_unique = False
        return is_unique

    def create_dir_link(self, file_link):
        file = self.files[0]
        links = file.links
        links.append("root/" + file_link)

    def truncate(self, fd_id, offset):
        for i in self.files:
            for j in i:
                if j == fd_id:
                    i.size = offset
                    i.file_blocks = self.set_file_blocks(i.size)
                    print("write out in" + str(i.name) + " " + str(offset) + " bytes")
                    return
        print("file doesnt exist")

    def file_stat(self, file_id):
        if len(self.files) == 0:
            print("file system not mounted")
            return
        for file in self.files:
            if file.file_id == file_id:
                print(file)

    def ls(self):
        if len(self.files) == 0:
            print("file system not mounted")
            return
        for file in self.files:
            if file.file_type != Type.LINK:
                print(file)

    def create(self, file_name):
        if len(self.files) == 0:
            print("file system not mounted")
            return
        if not self.is_name_unique(file_name):
            print("file with that name already exist")
            return
        file = SystemFile(Tools.generate_id(), Type.FILE, file_name, 0, [], [], [])
        self.files.append(file)
        self.create_dir_link(file_name)
        self.link(file_name, file_name)
        print("file created")
        self.api.save_to_file_system(self.files)

    def open(self, current_file_name: str):
        for file in self.files:
            if file.file_name == current_file_name and file.file_type == Type.FILE:
                fd = []
                if file.uuids is not None:
                    fd = file.uuids
                fd.append(Tools.generate_id())
        self.api.save_to_file_system(self.files)

    def close(self, close_file_uuid):
        for file in self.files:
            for file_uuid in file.uuids:
                if close_file_uuid == file_uuid:
                    file.uuids.remove(close_file_uuid)
                    print("file closed " + str(file.file_name))
                    self.api.save_to_file_system(self.files)
                    break
        print("file not found")

    def read(self, read_file_uuid, offset):
        is_break = False
        for file in self.files:
            if is_break:
                break
            if file.uuids is None:
                file.uuids = []
            for i in file.uuids:
                if i.hex == read_file_uuid:
                    if file.size < offset:
                        print("Out of file error " + file.file_name)
                    print(f"file read {file.file_name} {offset} bytes")
                    self.api.save_to_file_system(self.files)
                    is_break = True
                    break
        else:
            print("file not found")

    def write(self, write_file_uuid, offset):
        for i in self.files:
            if i.file_name == Type.FILE:
                continue
            for j in i.uuids:
                if j == write_file_uuid:
                    i.size = i.size + offset
                    i.file_blocks = self.set_file_blocks(i.size)
                    print("file write" + i.file_name + "" + offset + " bytes")
                    break
        print("file not found")

    def link(self, first_name, second_name):
        isFilePresent = False
        for i in self.files:
            if i.file_name == second_name and i.file_type == Type.FILE:
                i.links.append(first_name)
                isFilePresent = True
        if isFilePresent:
            newFile = SystemFile(Tools.generate_id(), Type.LINK, first_name, 0, [second_name], None, None)
            self.files.append(newFile)
            self.api.save_to_file_system(self.files)

    def unlink(self, name):
        fileToUnLink = None
        for i in self.files:
            if i.name == name and i.file_type == Type.LINK:
                fileToUnLink = i
        if fileToUnLink is not None:
            self.files.remove(fileToUnLink)

    def set_file_blocks(self, size):
        freeBlocks = []
        self.file_blocks.sort()
        if len(self.file_blocks) == 0:
            lastBlock = 0
        else:
            lastBlock = self.file_blocks[len(self.file_blocks) - 1]
        countOfBlocks = int(size / self.BYTES_FOR_BLOCK)
        for i in range(countOfBlocks):
            if len(self.file_blocks) == 0 and len(self.file_blocks) < self.file_blocks[len(self.file_blocks) - 1]:
                freeBlocks.append(self.next_free_block())
            else:
                lastBlock += 1
                freeBlocks.append(lastBlock)
        return freeBlocks


class Operation:
    def __init__(self, operation_type, first_operand, second_operand, system_driver: SystemDriver):
        self.type = operation_type
        self.first_operand = first_operand
        self.second_operand = second_operand
        self.system_driver = system_driver

    def execute(self):
        if self.type == OperationType.MOUNT:
            self.system_driver.mount()
        if self.type == OperationType.READ:
            self.system_driver.read(self.first_operand, int(self.second_operand))
        if self.type == OperationType.CREATE:
            self.system_driver.create(self.first_operand)
        if self.type == OperationType.LS:
            self.system_driver.ls()
        if self.type == OperationType.OPEN:
            self.system_driver.open(self.first_operand)


class OperationEngine:

    def __init__(self):
        self.system_driver = SystemDriver()

    def run(self):
        while True:
            command, operation = self.get_command()
            if operation is None:
                print(f"Command \"{command}\" not found")
                continue
            operation.execute()

    def get_command(self):
        com = input("Please, enter command to execute:\n")
        com_parts: [str] = com.split()
        if len(com_parts) == 1:
            if com_parts[0] == "mount":
                return com, Operation(OperationType.MOUNT, None, None, self.system_driver)
            if com_parts[0] == "unmount":
                return com, Operation(OperationType.UNMOUNT, None, None, self.system_driver)
            if com_parts[0] == "ls":
                return com, Operation(OperationType.LS, None, None, self.system_driver)
            if com_parts[0] == "w":
                return com, Operation(OperationType.W, None, None, self.system_driver)
        if len(com_parts) == 2:
            if com_parts[0] == "create":
                return com, Operation(OperationType.CREATE, com_parts[1], None, self.system_driver)
            if com_parts[0] == "filestat":
                return com, Operation(OperationType.FILE_STAT, com_parts[1], None, self.system_driver)
            if com_parts[0] == "open":
                return com, Operation(OperationType.OPEN, com_parts[1], None, self.system_driver)
            if com_parts[0] == "close":
                return com, Operation(OperationType.CLOSE, com_parts[1], None, self.system_driver)
            if com_parts[0] == "unlink":
                return com, Operation(OperationType.UNLINK, com_parts[1], None, self.system_driver)
        if len(com_parts) == 3:
            if com_parts[0] == "read":
                return com, Operation(OperationType.READ, com_parts[1], com_parts[2], self.system_driver)
            if com_parts[0] == "write":
                return com, Operation(OperationType.WRITE, com_parts[1], com_parts[2], self.system_driver)
            if com_parts[0] == "truncate":
                return com, Operation(OperationType.TRUNCATE, com_parts[1], com_parts[2], self.system_driver)
            if com_parts[0] == "link":
                return com, Operation(OperationType.LINK, com_parts[1], com_parts[2], self.system_driver)
        return com, None


print("Starting File System")
operation_engine = OperationEngine()
operation_engine.run()