import pymysql
import random

class FlinkMysqlDataGenerate:
    """
    flink 数据流处理需要，mysql数据生成
    """
    def __init__(self, host="127.0.0.1", user='root', passwd='debezium', dbName='es', charset='utf8'):
        self.host= host
        self.user= user
        self.passwd = passwd
        self.dbName = dbName
        self.charset = charset
        self.db = self.__connect_db()
        self.cursor = self.db.cursor()
        self.check_tables()
    def __connect_db(self):
        try:
            db = pymysql.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.dbName, charset=self.charset)
            return db
        except BaseException as e:
            raise BaseException("mysql connect failed:{}".format(e))

    def __insert_many(self, command, data):
        try:
            self.cursor.executemany(command, data)
            self.db.commit()
            print("当前插入数据{}条".format(len(data)))
            self.db.close()
            isInsert = True
        except pymysql.err.IntegrityError:
            print("Primary is already exists")
            isInsert = True
        except Exception as e:
            raise ("insert error.{}".format(e))
        return isInsert

    def check_tables(self):
        create_users_sql = """CREATE TABLE IF NOT EXISTS `users` (
                                  `id` varchar(40) NOT NULL,
                                  `name` varchar(256) NOT NULL,
                                  `age` int(11) DEFAULT NULL,
                                  `gender` enum('unknown','male','female') DEFAULT NULL,
                                  `ctime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                  `utime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                  PRIMARY KEY (`id`)
                                ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
        self.cursor.execute(create_users_sql)

        create_products_sql = """CREATE TABLE IF NOT EXISTS `products` (
                                  `id` varchar(40) NOT NULL,
                                  `title` varchar(256) NOT NULL,
                                  `price` bigint(20) DEFAULT NULL,
                                  `ctime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                  `utime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                  PRIMARY KEY (`id`)
                                ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
        self.cursor.execute(create_products_sql)

        create_orders_sql = """CREATE TABLE IF NOT EXISTS `orders` (
                                  `id` varchar(40) NOT NULL,
                                  `amount` bigint(20) DEFAULT NULL,
                                  `user_id` varchar(256) NOT NULL,
                                  `ctime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                  `utime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                  PRIMARY KEY (`id`)
                                ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
        self.cursor.execute(create_orders_sql)

        create_order_items_sql = """CREATE TABLE IF NOT EXISTS `order_items` (
                                      `id` varchar(40) NOT NULL,
                                      `product_id` varchar(40) NOT NULL,
                                      `order_id` varchar(40) NOT NULL,
                                      `price` bigint(20) NOT NULL,
                                      `quantity` int(11) NOT NULL,
                                      `discount` bigint(20) NOT NULL,
                                      `ctime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                      `utime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                      PRIMARY KEY (`id`),
                                      KEY `fk_order_id` (`order_id`),
                                      KEY `fk_product_id` (`product_id`),
                                      KEY `idx` (`id`) USING BTREE,
                                      CONSTRAINT `fk_order_id` FOREIGN KEY (`order_id`) REFERENCES `orders` (`id`),
                                      CONSTRAINT `fk_product_id` FOREIGN KEY (`product_id`) REFERENCES `products` (`id`)
                                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
        self.cursor.execute(create_order_items_sql)

    def UserData(self):
        """
        用户信息生成
        :return:
        """
        man = "male"
        woman = "female"
        manCount = 1
        womanCount = 1
        once_count = []
        sqlCommand = """INSERT INTO user1 (id, name, age, gender) VALUES(%s, %s, %s, %s)"""

        for i in range(1000000000,1000010000):
            user = []
            age = random.randint(15,45)
            if i % 2 == 0:
                name = "男性用户" + str(manCount)
                gender = man
                manCount += 1
            else:
                name = "女性用户" + str(womanCount)
                gender = woman
                womanCount += 1
            user.append(i)
            user.append(name)
            user.append(age)
            user.append(gender)
            once_count.append(user)
            if (i+1) % 10000 == 0:
                self.__insert_many(sqlCommand, once_count)
                once_count.clear()

    def PruductesData(self):
        """
        productes记录
        :return:
        """
        products = [[1, '测试帽子', 99], [2, '测试袜子', 288], [3, '测试上衣', 1888], [4, '测试外套', 599]]
        sqlCommand = """INSERT INTO products (id, title, price) VALUES (%s, %s, %s)"""
        self.__insert_many(sqlCommand, products)

    def OrdersData(self, start, end):
        """
        orders记录
        :param start: 起始键
        :param end: 结束键
        :return:
        """
        insert_data = []
        sqlCommand = 'INSERT INTO orders (id,amount,user_id) VALUES(%s, %s, %s);'
        for i in range(start, end):
            element_data = []
            element_data.append(i)
            element_data.append(random.randint(1,50))
            element_data.append(random.randint(1000000000, 1000010000))
            insert_data.append(element_data)
            if i % 100000 == 0:
                self.__insert_many(sqlCommand, insert_data)
                insert_data.clear()

    def orderItemsData(self,start, end):
        """
        order_items记录
        :param start: 起始键
        :param end: 结束键
        :return:
        """
        p = {"1":99, "2":288, "3":1888, "4":599}
        insert_data = []
        sqlCommand = 'INSERT INTO order_items (id, product_id, order_id, price, quantity, discount) VALUES(%s, %s, %s, %s, %s, %s);'
        for i in range(start, end):
            element_data = []
            product = random.randint(1,3)
            element_data.append(str(i))
            element_data.append(str(product))
            element_data.append(str(i))
            element_data.append(p.get(str(product)))
            element_data.append(random.randint(1, 30))
            element_data.append(random.randint(10,100))
            insert_data.append(element_data)
            if i % 100000 == 0:
                self.__insert_many(sqlCommand, insert_data)
                insert_data.clear()


if __name__ == "__main__":
    dataGenerate = FlinkMysqlDataGenerate()
    # dataGenerate.UserData()
    # dataGenerate.PruductesData()
    # dataGenerate.OrdersData(1,100000000)
    # dataGenerate.orderItemsData(1,100000000)
