/**
 * @param arg - value to check
 * @returns whether the given value is defined
 */
export function isDefined<T>(arg: T | undefined | null): arg is T {
  return arg !== undefined && arg !== null;
}
