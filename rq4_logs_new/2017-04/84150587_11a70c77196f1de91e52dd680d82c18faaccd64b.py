from AlgorithmTraining.data_structure.HashTable.SeprateLinkHash import HashNode
class NextAddr:
    """
    产生下一个偏移地址的迭代器
    """
    def __init__(self):
        self.status = 0
        self.query_status = 0

    def get_next_offset(self):
        self.status += 1
        return self.status

    def get_next_query_offset(self):
        self.query_status += 1
        return self.query_status

class HashTable_OpenAddr:
    """
    开放定址法的哈希表
    """
    def __init__(self, table_size=10, hash_function=None, lambda_val=None):
        """
        在创建哈希表时就指定好表的大小
        :param table_size:
        """
        self.table_size = table_size  # 表的总空间
        self.hashed_size = 0  # 当前表中已有的元素
        self.table = self.create_hash_table(table_size=table_size)
        # 哈希值计算函数 两个参数一个是要哈希的关键字 另一个是哈希表的大小
        self.hash_fun = hash_function if hash_function is not None else self.hash_fun0
        self.lambda_val = 0.4 if lambda_val is None else lambda_val

    def hash_fun0(self, keyword, table_size=10):
        """
        :param keyword:要插入到哈希表中的关键字
        :param table_size:
        :return: 返回求得的哈希值
        暂时使用求余数运算 是一个可以替换的函数
        """
        return keyword % table_size

    def find_next_empty_addr(self, this_addr):
        """
        计算下一个空的存放地址 用于插入操作
        :param this_addr:
        :param max_addr:
        :param next_addr: 计算下一个可用理论地址的函数 可以为线性或者二次
        :return: 计算出来的下一个可用地址
        """
        new_addr = this_addr
        while self.table[this_addr] is not None:  # 这个地址被占用了
            next_calc = NextAddr()
            new_addr = this_addr+next_calc.get_next_offset()
            new_addr = new_addr % self.table_size  # 如果获得新地址大于哈希表的实际大小
            #  就通过求余函数回环
            this_addr = new_addr
        return new_addr

    def create_hash_table(self, table_size=10):
        """
        创建了一个空的表 等待后续的链接
        :param table_size:
        :return:
        """
        list0 = [None, ]*table_size
        return list0

    def insert(self, keyword, val):
        """
        将关键字插入到哈希表中指定的位置
        对于相同的哈希映射 关键字 插入到链表头部
        :param keyword:
        :return:
        """
        hashed_key = self.hash_fun(keyword=keyword, table_size=self.table_size)
        tmp = HashNode(keyword=keyword, val=val)
        if self.table[hashed_key] is not None:
            new_hashed_key = self.table[self.find_next_empty_addr(hashed_key)] # 将新的节点插入到链表的头部 最新插入的数据最可能被用到
        else:
            new_hashed_key = hashed_key
        self.table[new_hashed_key] = tmp
        self.hashed_size += 1
        if self.hashed_size/self.table_size > self.lambda_val:
            self.rehash()

    def query(self, keyword):
        """
        根据关键字查询到相关的单元 返回相关的单元的值的引用
        :param key_work:
        :return: handler if find else None
        """
        hashed_key = self.hash_fun(keyword=keyword, table_size=self.table_size)
        if self.table[hashed_key] is not None:
            tmp = self.table[hashed_key]
            query_offset_calc = NextAddr()
            while tmp.keyword != keyword and (tmp is not None):
                hashed_key += query_offset_calc.get_next_query_offset()
                tmp = self.table[hashed_key]
            return None if tmp is None else tmp
        else:  # 直接就没有找到 直接就返回None
            return None

    def rehash(self):
        """
        当超过哈希表的装填因子lambda时 触发再哈希
        :return:
        """
        print("rehash invoked!", self.table_size)
        new_hash_table = HashTable_OpenAddr(table_size=2*self.table_size, hash_function=self.hash_fun)
        for keyword_head in self.table:
            tmp = keyword_head
            if tmp is not None:
                new_hash_table.insert(keyword=tmp.keyword,val=tmp.val)
        self.table = new_hash_table.table
        self.table_size = new_hash_table.table_size
        self.hashed_size = new_hash_table.hashed_size




