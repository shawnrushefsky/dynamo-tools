function toObject(dynamoItem) {
  if (typeof dynamoItem === "undefined") {
    return;
  }

  if (!Array.isArray(dynamoItem) && typeof dynamoItem !== "object") {
    return dynamoItem;
  }

  if (typeof dynamoItem.S !== "undefined") {
    return dynamoItem.S.toString();
  }

  if (typeof dynamoItem.N !== "undefined") {
    return Number(dynamoItem.N);
  }

  if (dynamoItem.B) {
    return Buffer.from(dynamoItem.B, "base64");
  }

  if (dynamoItem.hasOwnProperty("BOOL")) {
    return dynamoItem.BOOL;
  }

  if (dynamoItem.NULL) {
    return null;
  }

  if (dynamoItem.M) {
    const obj = {};
    Object.keys(dynamoItem.M).forEach((key) => {
      obj[key] = toObject(dynamoItem.M[key]);
    });
    return obj;
  }

  if (dynamoItem.L) {
    return dynamoItem.L.map(toObject);
  }

  if (dynamoItem.SS) {
    return new Set(dynamoItem.SS.map(String));
  }

  if (dynamoItem.NS) {
    return new Set(dynamoItem.NS.map(Number));
  }

  if (dynamoItem.BS) {
    return new Set(dynamoItem.BS.map((blob) => Buffer.from(blob, "base64")));
  }

  if (Array.isArray(dynamoItem)) {
    return dynamoItem.map(toObject);
  }

  if (typeof dynamoItem === "object") {
    const obj = {};
    Object.keys(dynamoItem).forEach((key) => {
      obj[key] = toObject(dynamoItem[key]);
    });
    return obj;
  }
}

function fromObject(obj) {
  if (typeof obj === "undefined") {
    return;
  }

  if (typeof obj === "string" && !/^\d+$/.test(obj)) {
    return { S: obj };
  }

  if (
    typeof obj === "number" ||
    (typeof obj === "string" && !isNaN(obj) && !isNaN(parseFloat(obj)))
  ) {
    return { N: obj.toString() };
  }

  if (Buffer.isBuffer(obj)) {
    return { B: obj.toString("base64") };
  }

  if (typeof obj === "boolean") {
    return { BOOL: obj };
  }

  if (obj === null) {
    return { NULL: true };
  }

  if (Array.isArray(obj)) {
    if (isPOJO(obj[0])) {
      return { L: obj.map((elem) => ({ M: fromObject(elem) })) };
    }
    return { L: obj.map(fromObject) };
  }

  if (obj instanceof Set) {
    const member = obj.values().next().value;
    if (typeof member === "string") {
      return { SS: Array.from(obj) };
    }

    if (typeof member === "number") {
      return { NS: Array.from(obj) };
    }

    if (Buffer.isBuffer(member)) {
      return { BS: Array.from(obj).map((buff) => buff.toString("base64")) };
    }
  }

  if (typeof obj === "object") {
    const item = {};
    Object.keys(obj).forEach((key) => {
      if (isPOJO(obj[key])) {
        item[key] = { M: fromObject(obj[key]) };
      } else {
        item[key] = fromObject(obj[key]);
      }
      if (typeof item[key] === "undefined") {
        delete item[key];
      }
    });
    return item;
  }
}

function isPOJO(obj) {
  return (
    !Array.isArray(obj) &&
    !Buffer.isBuffer(obj) &&
    !(obj instanceof Set) &&
    obj !== null &&
    typeof obj === "object"
  );
}

module.exports = {
  toObject,
  fromObject,
  isPOJO,
};
