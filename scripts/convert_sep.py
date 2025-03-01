import os, argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--path', required=True)
    args = parser.parse_args()

    with open(args.path, 'r') as f_in:
        with open(args.path + '.tmp', 'w') as  f_out:
            f_in.readline()
            for line in f_in:
                x = line.strip().split('|')
                f_out.write(' '.join(x) + '\n')
    os.remove(args.path)
    os.rename(args.path + '.tmp', args.path)
