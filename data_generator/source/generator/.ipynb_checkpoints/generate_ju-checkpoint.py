{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {},
   "outputs": [],
   "source": [
    "import random\n",
    "import csv"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 24,
   "metadata": {},
   "outputs": [],
   "source": [
    "def generate(start_point=1,end_point=10,random_duplicates=False, number_of_duplicates=5,beign_rand=1, end_rand=20, percent=20):\n",
    "    count=end_point-start_point+1\n",
    "    fraction=count*percent\n",
    "    list_row=[]\n",
    "    list_row.append(([\"E\",\"F\"]))\n",
    "    i=1\n",
    "    if random_duplicates is False:\n",
    "        for j in range(fraction):\n",
    "            rand_index= random.randrange(start_point,end_point)\n",
    "            for k in range(number_of_duplicates):\n",
    "                list_row.append([\"e\"+str(rand_index), \"f\"+str(rand_index)])\n",
    "                i=i+1\n",
    "    else \n",
    "    for j in range(start_point,end_point):\n",
    "        #rand_num = random.randrange(beign_rand,end_rand)\n",
    "        for k in range(rand_num):\n",
    "            list_row.append([\"e\"+str(i), \"f\"+str(j)])\n",
    "            i=i+1\n",
    "    new_file = \"C:\\\\Users\\\\TorabinejadM\\\\Desktop\\\\Thesis\\\\implementation\\\\data-generator\\\\source\\\\generator\\\\test_data2.csv\"\n",
    "    with open(new_file, 'w') as f_out:\n",
    "        writer = csv.writer(f_out)\n",
    "        writer.writerows(list_row)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 25,
   "metadata": {},
   "outputs": [],
   "source": [
    "generate()"
   ]
  },
  {
   "cell_type": "raw",
   "metadata": {},
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.4"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
