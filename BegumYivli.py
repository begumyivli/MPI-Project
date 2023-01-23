# Student Name: Begüm Yivli - Kutay Saran
# Student Number: 2019400147 - 2015400204
# Compile Status: Compiling
# Program Status: Working
# Notes: WORKERS method may be little slow but its working

from mpi4py import MPI
import argparse

comm = MPI.COMM_WORLD
world_size = comm.Get_size()  # number of workers + master
num_workers = world_size - 1  # number of workers
rank = comm.Get_rank()  # which processor works

parser = argparse.ArgumentParser()  # parser for arguments
parser.add_argument('--input_file')
parser.add_argument('--merge_method')
parser.add_argument('--test_file')
args = parser.parse_args()

f = open(args.input_file, "r")  # data file
merge_method = args.merge_method  # MASTER or WORKERS method
test = open(args.test_file, "r")  # test file

with open(args.input_file, 'r') as fp:
    total_lines = len(fp.readlines())  # how many sentences in total

# these dictionaries for master process, all sentences' data is gathered here
master_dict = {}
master_uni = {}
master_bi = {}
master_dict[1] = master_uni
master_dict[2] = master_bi


# requirement 2
def req2():
    # I create a dictionary for every process, processes parse the sentences they receive,
    # count the bigram and unigram's and save in this dictionaries
    my_dict = {}
    uni_dict = {}
    bi_dict = {}
    my_dict[1] = uni_dict
    my_dict[2] = bi_dict
    num_of_sentences = 0
    # every process receive a list which contains lines(string) from master
    data = comm.recv(source=0)
    for sentence in data:
        num_of_sentences += 1
        # sentence is a string so, I split it to get every word
        my_word_list = sentence.split()
        # this part calculates unigrams and save how many unigram
        # if this process' dictionary does not have this word it creates a key for this word
        # if dictionary has word it increments the value:
        for word in my_word_list:
            if word not in uni_dict:
                uni_dict[word] = 1
            else:
                uni_dict[word] += 1
        # this part calculates bigrams and save how many bigrams
        # if this process' dictionary does not have this bigram it creates a key for this bigram
        # if dictionary has bigram it increments the value:
        for b in range(len(my_word_list) - 1):
            # bigrams are created by combining every 2 word in the sentence
            bigram = my_word_list[b] + " " + my_word_list[b + 1]
            if bigram not in bi_dict:
                bi_dict[bigram] = 1
            else:
                bi_dict[bigram] += 1

    # if lines are more than k*num_workers, k is a positive number
    # then there is one more sentence for that process
    if rank <= total_lines % num_workers:
        num_of_sentences += 1
        # unigram operations:
        for word in data[-1].split():
            if word not in uni_dict:
                uni_dict[word] = 1
            else:
                uni_dict[word] += 1

        # bigram operations:
        for b in range(len(data[-1].split()) - 1):
            bigram = data[-1].split()[b] + " " + data[-1].split()[b + 1]
            if bigram not in bi_dict:
                bi_dict[bigram] = 1
            else:
                bi_dict[bigram] += 1

    comm.send(my_dict, dest=0)  # every process send its data to the master
    print("rank=", rank)  # we print to the console rank of the process
    # and how many sentence receive it
    print(num_of_sentences)


# requirement 3
def req3():
    num_of_sentences = 0
    # if process is the last worker
    if rank == num_workers:
        # if there is 1 worker the last and the first is same so, we create a dict for
        # saving how many bigram and unigram are there in this process' lines
        if num_workers == 1:
            my_dict = {}
            uni_dict = {}
            bi_dict = {}
            my_dict[1] = uni_dict
            my_dict[2] = bi_dict
        # if there is more than 1 process last worker is not first
        # so, the process receives the predecessor process' dict
        else:
            my_dict = comm.recv(source=rank - 1)
            uni_dict = my_dict[1]
            bi_dict = my_dict[2]

    # if process is the first worker we create a dict for
    # saving how many bigram and unigram are there in this process' lines
    elif rank == 1 and num_workers > 1:
        my_dict = {}
        uni_dict = {}
        bi_dict = {}
        my_dict[1] = uni_dict
        my_dict[2] = bi_dict

    # if the process is not last worker so, the process receives the predecessor process' dict
    else:
        my_dict = comm.recv(source=rank - 1)
        uni_dict = my_dict[1]
        bi_dict = my_dict[2]

    # every process receive a list which contains lines(string) from master
    data = comm.recv(source=0)
    for sentence in data:
        num_of_sentences += 1
        # sentence is a string so, I split it to get every word
        my_word_list = sentence.split()
        # this part calculates unigrams and save how many unigram
        # if this process' dictionary does not have this word it creates a key for this word
        # if dictionary has word it increments the value:
        for word in my_word_list:
            if word not in uni_dict:
                uni_dict[word] = 1
            else:
                uni_dict[word] += 1
        # this part calculates bigrams and save how many bigrams
        # if this process' dictionary does not have this bigram it creates a key for this bigram
        # if dictionary has bigram it increments the value:
        for b in range(len(my_word_list) - 1):
            # bigrams are created by combining every 2 word in the sentence
            bigram = my_word_list[b] + " " + my_word_list[b + 1]
            if bigram not in bi_dict:
                bi_dict[bigram] = 1
            else:
                bi_dict[bigram] += 1

    # if lines are more than k*num_workers, k is a positive number
    # then there is one more sentence for that process
    if rank <= total_lines % num_workers:
        num_of_sentences += 1
        # unigram operations:
        for word in data[-1].split():
            if word not in uni_dict:
                uni_dict[word] = 1
            else:
                uni_dict[word] += 1

        # bigram operations:
        for b in range(len(data[-1].split()) - 1):
            bigram = data[-1].split()[b] + " " + data[-1].split()[b + 1]
            if bigram not in bi_dict:
                bi_dict[bigram] = 1
            else:
                bi_dict[bigram] += 1

    # if the process is last worker it sends its data(dict) to master
    if rank == num_workers:
        comm.send(my_dict, dest=0)

    # if the process is not last worker it sends its data(dict) to successor process
    else:
        comm.send(my_dict, dest=rank + 1)

    print("rank=", rank)  # we print to the console rank of the process
    # and how many sentence receive it
    print(num_of_sentences)


# requirement 1:
if rank == 0:
    whole = total_lines // num_workers
    # here I calculate the line number to be sent every process
    # I open the data file and make a string list which length is
    # the number ı calculated. Then I send this list to processes
    with open(args.input_file, 'r') as fp:
        for j in range(1, num_workers + 1):
            my_range = whole
            head = [next(fp) for x in range(my_range)]
            comm.send(head, dest=j)

else:
    if merge_method == "MASTER":  # if the arg is master, I call req2
        # in req2 every process make operation its own data and
        # send the result to the master
        req2()
    elif merge_method == "WORKERS":  # if the arg is master, I call req3
        # in req3 every process make operation its own data and
        # send the result to the successor process
        req3()

# requirement 4
if rank == 0:
    if merge_method == "MASTER":
        # if the merge method is MASTER, then master process receive data from every process
        # and save bigram and unigram counts to its own dictionary
        for i in range(1, num_workers + 1):
            data = comm.recv(source=i)
            unidict = data[1]
            bidict = data[2]
            for key in unidict:
                if key not in master_uni:
                    master_uni[key] = unidict[key]
                else:
                    master_uni[key] += unidict[key]

            for key in bidict:
                if key not in master_bi:
                    master_bi[key] = bidict[key]
                else:
                    master_bi[key] += bidict[key]
    else:
        data = comm.recv(source=num_workers)
        # if the merge method is WORKERS, then master process receive data from last process
        # and save bigram and unigram counts to its own dictionary
        unidict = data[1]
        bidict = data[2]
        for key in unidict:
            if key not in master_uni:
                master_uni[key] = unidict[key]
            else:
                master_uni[key] += unidict[key]

        for key in bidict:
            if key not in master_bi:
                master_bi[key] = bidict[key]
            else:
                master_bi[key] += bidict[key]

    # we count the number of lines in test file for "for" loop
    with open(args.test_file, 'r') as fp:
        total = len(fp.readlines())

    # in for loop i divide the line frequency to the first word frequency
    # for finding bigram probability because
    # P(A|B) = Freq(A*B) / Freq(B)
    for i in range(total):
        line = test.readline().strip()
        first = line.split()[0]
        print((master_bi[line]) / (master_uni[first]))
