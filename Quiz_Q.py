def complex_lst_comprehension():
    x = 2
    y = 3
    z = 4
    a = [[i,j] for i in range(x+1) for j in range(x+1)
        if i+j <= 5].sort
    print(a)

def lam_da():
    lst = [1,2,3,4,5,6,7,8,9]
    new_lst = []
    for x in lst:
        if x % 2 == 0 :
            new_lst.append(x)
    print(new_lst)

    alt_lst = list[filter(lambda x:x%2==0,lst)]
    print(alt_lst)