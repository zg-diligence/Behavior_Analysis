
    tmp_result/文件夹说明:
        user_graph:
            根据每个选定用户在整个5月的所有事件cycle构建的事件关系图, 每个cycle是一个用户一段时间内的连续事件, 相邻事件之间有有一条有向边
            同一用户在一段时间内的连续事件随机序列相同, 以此标识cicyle
            根据不同的阈值可以得到不同的有向图
            除此之外, total_usrs.png 是所有用户整个五月份的事件关系图

        user_pattern:
            每个选定用户在五月份所有的行为模式
            行为模式分为三种:
                浏览行为    look-through
                时移行为    time-shift
                VOD点播行为 vod-play

        prefer_analysis:
            根据用户整个五月份的收视数据分析得到用户的收视喜好, 饼状图表示

    tmp_result/文件说明:
        choosed_usrs.txt: 49个选定的用户
        src_programs_dict.txt: 初始节目分类结果, 其中有奇异项, 即一个节目对应多个分类
        nprograms_dict.txt: 调整后的节目分类列表, 格式: 类别\t\t节目
