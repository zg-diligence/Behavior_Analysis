    tmp_result/文件夹说明:
        classifed_result/: 每一步的分类结果
        extract_chanel_program/: 从源数据中提取的节目或<频道,节目>不重复数据, 按事件分类
        scrapy_programs/: 从网站爬取的各类金标准节目(已分类)

    tmp_result/文件说明:
        BA_all_channels.txt: 所有有效频道
        BA_manual_classified_channels.txt: 所有可以预分类的频道,不精确,以basic_category.py中为准
        keyword_entertainment.txt: 娱乐明星名字, 用于关键词分类

        normalized_programs.txt: 所有提取的节目, 预处理后
        normalized_channels.txt: 所有提取的频道, 预处理后
        original_unique_channels.txt: 所有提取的频道, 预处理前
        original_unique_programs.txt: 所有提取的节目, 预处理后

        prefix_programs.txt: 第二步分类中所有提取的前缀
        prefix_lists.txt: 第二步分类中左右提取的前缀以及相应的节目
        prefix_classifed_result.txt: 第二部分中可分类前缀的分类结果
		programs_classified.txt: 最终分类结果, 分类标志|节目类别|节目名称, 具体细节见分类算法

    根目录下文件说明:
        classification_algorithm.txt: 分类算法
        其余文件功能详见分类算法和文件注释
