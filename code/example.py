with open("../dataset/ratings.csv",'r') as file:
    count = 1
    next(file)
    for i,line in enumerate(file):
        print(line)
        if i > 10:
            break

with open("output.csv", "w+") as fileOutput:
    fileOutput.write("userId,movieId,rating,timestamp")
    for line in file:
        fileOutput.write(line)

