def number_of_patterns(m, n):
    exceptions = {
            (1, 3): 2,
            (3, 9): 6,
            (7, 9): 8,
            (1, 7): 4,
            (2, 8): 5,
            (4, 6): 5,
            (1, 9): 5,
            (3, 7): 5,
        }   
    
    total = 0

    def recur_pattern(idx, step, path):
        nonlocal total 

        if n<step:
            return 
        
        if m<=step and step<=n:
            total += 1

        path.add(idx)

        for next in range(1,10):
            if next in path:
                continue 
            elif (min(idx, next), max(idx, next)) in exceptions and exceptions[(min(idx, next), max(idx, next))] not in path:
                continue 

            recur_pattern(next, step+1, path)
        
        path.remove(idx)
        return

    for i in range(1,10):
        recur_pattern(i, 1, set())

    return total
