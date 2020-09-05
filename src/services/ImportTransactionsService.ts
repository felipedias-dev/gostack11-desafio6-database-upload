import csvParse from 'csv-parse';
import fs from 'fs';

import { In, getCustomRepository, getRepository } from 'typeorm';
import Transaction from '../models/Transaction';
import AppError from '../errors/AppError';
import TransactionsRepository from '../repositories/TransactionsRepository';
import Category from '../models/Category';

interface TransactionCSV {
  title: string;
  value: number;
  type: 'income' | 'outcome';
  category: string;
}

class ImportTransactionsService {
  async execute(csvFilePath: string): Promise<Transaction[]> {
    const transactionsRepository = getCustomRepository(TransactionsRepository);
    const categoriesRepository = getRepository(Category);
    const readCSVStream = fs.createReadStream(csvFilePath);
    const parseStream = csvParse({
      from_line: 2,
      ltrim: true,
      rtrim: true,
    });
    const parseCSV = readCSVStream.pipe(parseStream);
    const transactions: TransactionCSV[] = [];
    const categories: string[] = [];

    parseCSV.on('data', async line => {
      const [title, type, value, category] = line.map((cell: string) =>
        cell.trim(),
      );

      if (!title || !value || !type) {
        throw new AppError('There is at least one data missing!');
      }

      transactions.push({ title, value, type, category });
      categories.push(category);
    });

    await new Promise(resolve => parseCSV.on('end', resolve));

    const setCategories = new Set(categories);
    const uniqueCategories = Array.from(setCategories);

    const existentCategories = await categoriesRepository.find({
      where: { title: In(categories) },
    });
    const existentCategoryTitles = existentCategories.map(
      (category: Category) => category.title,
    );
    const addCategoryTitles = uniqueCategories.filter(
      category => !existentCategoryTitles.includes(category),
    );
    const newCategories = categoriesRepository.create(
      addCategoryTitles.map(title => ({ title })),
    );

    await categoriesRepository.save(newCategories);

    const allCategories = [...newCategories, ...existentCategories];
    const createdTransactions = transactionsRepository.create(
      transactions.map(transaction => ({
        title: transaction.title,
        value: transaction.value,
        type: transaction.type,
        category: allCategories.find(
          category => category.title === transaction.category,
        ),
      })),
    );

    await transactionsRepository.save(createdTransactions);
    await fs.promises.unlink(csvFilePath);

    return createdTransactions;
  }
}

export default ImportTransactionsService;
