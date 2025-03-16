module.exports = {
  extends: ['@commitlint/config-conventional'],
  rules: {
    'type-enum': [
      2,
      'always',
      [
        'build',    // Changes that affect the build system or external dependencies
        'chore',    // Regular maintenance tasks
        'ci',       // Changes to CI configuration files and scripts
        'docs',     // Documentation only changes
        'feat',     // A new feature
        'fix',      // A bug fix
        'perf',     // A code change that improves performance
        'refactor', // A code change that neither fixes a bug nor adds a feature
        'revert',   // Reverts a previous commit
        'style',    // Changes that do not affect the meaning of the code
        'test'      // Adding missing tests or correcting existing tests
      ]
    ],
    'scope-case': [2, 'always', 'kebab-case'],
    'subject-case': [2, 'always', 'lower-case'],
    'subject-empty': [2, 'never'],
    'subject-full-stop': [2, 'never', '.'],
    'type-case': [2, 'always', 'lower-case'],
    'type-empty': [2, 'never']
  }
};
