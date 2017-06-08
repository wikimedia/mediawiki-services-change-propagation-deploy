module.exports = {
  setnx_pexpire:
    "local result = redis.call('SETNX', KEYS[1], ARGV[1])\n" +
    "if result == 1 then\n" +
    "  redis.call('PEXPIRE', KEYS[1], ARGV[2])\n" +
    "end\n" +
    "return result",

  delequal:
    "local deleted = 0\n" +
    "for i = 1, #KEYS, 1 do\n" +
    "  if redis.call('GET', KEYS[i]) == ARGV[i] then\n" +
    "    redis.call('DEL', KEYS[i])\n" +
    "    deleted = deleted + 1\n" +
    "  end\n" +
    "end\n" +
    "return deleted",

  zmembers:
    "local key = KEYS[1]\n" +
    "local results = {}\n" +
    "\n" +
    "if redis.call('ZCARD', key) == 0 then\n" +
    "  return {}\n" +
    "end\n" +
    "\n" +
    "for i = 1, #ARGV, 1 do\n" +
    "  local memberName = ARGV[i]\n" +
    "  if redis.call('ZSCORE', key, memberName) then\n" +
    "    table.insert(results, memberName)\n" +
    "  end\n" +
    "end\n" +
    "\n" +
    "return results;",

  znotmembers:
    "local key = KEYS[1]\n" +
    "local results = {}\n" +
    "\n" +
    "if redis.call('ZCARD', key) == 0 then\n" +
    "  return ARGV\n" +
    "end\n" +
    "\n" +
    "for i = 1, #ARGV, 1 do\n" +
    "  local memberName = ARGV[i]\n" +
    "  if not redis.call('ZSCORE', key, memberName) then\n" +
    "    table.insert(results, memberName)\n" +
    "  end\n" +
    "end\n" +
    "\n" +
    "return results;"
};
